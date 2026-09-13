```markdown
# Distributed GenAI Workflow & Job Execution Platform

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green.svg)](https://fastapi.tiangolo.com/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16%2Bpgvector-blue.svg)](https://www.postgresql.org/)
[![Redis 7.4](https://img.shields.io/badge/Redis-7.4%20Streams-red.svg)](https://redis.io/)
[![SQLAlchemy 2.0](https://img.shields.io/badge/SQLAlchemy-2.0%20Async-black.svg)](https://www.sqlalchemy.org/)

An enterprise-grade, event-driven background task execution platform designed for multi-stage GenAI pipelines (Document Ingestion -> Parsing & Chunking -> Vector Embedding -> Indexing). 

Engineered to guarantee **zero duplicate task execution** under concurrent bursts, **resilience against worker crashes** without repeating costly LLM inferences, and **dual-write consistency** across relational and message brokers.

---

## Architectural Highlights

* **Distributed Idempotency Layer:** Multi-tier idempotency combining sub-millisecond Redis atomic reservations (`SET key NX EX`) with durable PostgreSQL state lookups, eliminating redundant LLM inferences on network retries.
* **Transactional Outbox Pattern:** Solves the dual-write problem between PostgreSQL and Redis Streams. State updates and events are committed atomically inside PostgreSQL, then reliably dispatched via an asynchronous loop utilizing row-level locks (`SELECT ... FOR UPDATE SKIP LOCKED`).
* **Checkpointed Pipeline Resumption:** Each workflow stage checkpoints intermediate output to PostgreSQL. If a worker process crashes mid-pipeline, the claiming worker reads existing checkpoints and resumes execution at the point of failure.
* **Self-Healing Consumer Groups:** Background workers pull jobs via Redis Streams (`XREADGROUP`). Crashed or timed-out workers are automatically detected via consumer heartbeats and re-claimed using Redis 6.2+ `XAUTOCLAIM`.
* **Poison Pill Isolation (DLQ):** Messages exceeding max retry thresholds (`JOB_MAX_RETRIES = 3`) are automatically evicted from active consumer groups and routed to a Dead-Letter Queue (`jobs:dlq`) for manual triaging.
* **Full-Stack Telemetry:** OpenMetrics-compliant exporter surfacing real-time Redis Stream queue depth, step-level latency histograms, worker counts, and idempotency hit rates.

---

## System Architecture

```text
SYSTEM TOPOLOGY

[ HTTP Ingestion Client / Gateway Caller ]
                    │
                    ▼ (POST /v1/jobs with X-Idempotency-Key)
┌──────────────────────────────────────────────────────────────┐
│                      FastAPI Gateway                         │
│  • Redis Idempotency Guard (SET key PENDING NX EX 60)        │
│  • Atomic PostgreSQL Transaction:                            │
│      - INSERT INTO jobs (status = 'queued')                  │
│      - INSERT INTO outbox_events (status = 'pending')        │
│  • Redis Idempotency Finalize (SET key <job_id> EX 86400)     │
│  • Prometheus Metrics Exporter (/metrics)                    │
└───────────────┬──────────────────────────────▲───────────────┘
                │                              │
     (Polled via SKIP LOCKED)                  │ (Scrapes /metrics)
                ▼                              │
┌──────────────────────────────┐               │
│   Outbox Dispatcher Engine   │               │
│   (XADD jobs:stream MAXLEN~) │               │
└───────────────┬──────────────┘               │
                │                              │
                ▼                              │
┌──────────────────────────────────────────────────────────────┐
│                 Redis Streams Fabric (7.4)                   │
│   Stream: `jobs:stream` | Consumer Group: `job_workers`      │
│   DLQ Stream: `jobs:dlq`                                     │
└───────┬──────────────────────────────────────────────┬───────┘
        │                                              │
(XREADGROUP >)                               (XAUTOCLAIM min_idle=8s)
        ▼                                              ▼
┌──────────────────────────────┐               ┌──────────────────────────────┐
│   Worker Node A (Active)     │               │   Worker Node B (Recovery)   │
│   • Heartbeat: Redis TTL     │               │   • Reclaims idle PEL tasks  │
│   • Step 1: Parsing ──► DB   │               │   • Checks `workflow_steps`  │
│   • Step 2: Embed   ──► DB   │ ──(CRASH!)──► │   • Skips Steps 1 & 2        │
│   • Step 3: Index   ──► DB   │               │   • Resumes Step 3 ──► DB    │
│   • XACK jobs:stream         │               │   • XACK jobs:stream         │
└──────────────────────────────┘               └──────────────────────────────┘

```

---

## Concurrency & Stress Test Benchmarks

Simulated burst submissions with 100 requests across 20 concurrent connections, intentionally injecting a 20% duplicate idempotency key rate:

| Metric | Result |
| --- | --- |
| **Total Dispatched Requests** | 100 |
| **Total Ingestion Window** | 4.49 seconds |
| **Gateway Throughput** | **22.27 requests/sec** |
| **Successful New Jobs (HTTP 202)** | 80 |
| **Idempotency Duplicate Detections** | 20 (100% collision prevention) |
| **Failed / Dropped Requests** | **0** |
| **Gateway Latency (P50)** | 405.98 ms |
| **Gateway Latency (P95)** | 2,412.78 ms |
| **Gateway Latency (P99)** | 2,452.28 ms |
| **Queue Absorbed Depth** | 82 messages (0 dropped) |

---

## State Machine & Checkpointing Lifecycle

```text
       ┌──────────┐
       │  QUEUED  │ (Postgres committed, Outbox dispatches to Redis)
       └────┬─────┘
            │
            ▼
       ┌──────────┐
       │ RUNNING  │ (Worker claims task, updates job_attempts)
       └────┬─────┘
            │
┌────────────┴────────────┐
▼                         ▼
[Step Checkpoint Loop]    [Worker Crashes]
├─ Step 1: Parse          │
├─ Step 2: Embed          ▼ (XAUTOCLAIM detects idle PEL > 8s)
└─ Step 3: Index     [Reclaimed by Healthy Worker]
│                    │ (Reads completed steps from DB)
▼                    ▼
┌──────────┐         [Skips Steps 1 & 2, Resumes Step 3]
│SUCCEEDED │              │
└──────────┘              ▼
                     ┌──────────┐
                     │SUCCEEDED │
                     └──────────┘

```

---

## Quickstart & Local Setup

### 1. Prerequisites

* Python 3.12+
* Docker & Docker Compose

### 2. Infrastructure Spin-up

```bash
git clone [https://github.com/yourusername/genai-execution-engine.git](https://github.com/yourusername/genai-execution-engine.git)
cd genai-execution-engine

# Start PostgreSQL 16 (pgvector) and Redis 7.4
docker compose up -d

```

### 3. Environment & Migrations

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
alembic upgrade head

```

### 4. Running the Cluster Locally

**Terminal 1 (API Gateway & Outbox Dispatcher):**

```bash
uvicorn app.main:app --reload --port 8000

```

**Terminal 2 (Consumer Group Worker):**

```bash
python run_worker.py

```

**Terminal 3 (Submit a Job):**

```bash
python submit_job.py my-unique-key-001

```

**Terminal 4 (Run Stress Test Benchmark):**

```bash
python benchmark.py

```

```

---
