Markdown

# Fault-Tolerant GenAI Workflow Execution Engine

An enterprise-grade, distributed task execution engine built with **FastAPI**, **PostgreSQL** (`pgvector`), and **Redis Streams**. Designed to solve dual-write anomalies, enforce strict idempotency, recover state granularly across worker crashes, and expose real-time operational telemetry.

---

## 🏗️ System Architecture & Topology

              [ HTTP Ingestion Client / Gateway Caller ]
                                  │
                                  ▼ (POST /v1/jobs + X-Idempotency-Key)
┌──────────────────────────────────────────────────────────────┐
│                      FastAPI Gateway                         │
│  • Redis Idempotency Guard (SET key PENDING NX EX 60)        │
│  • Atomic PostgreSQL Transaction:                            │
│      - INSERT INTO jobs (status = 'queued')                  │
│      - INSERT INTO outbox_events (status = 'pending')        │
│  • Redis Idempotency Finalize (SET key <job_id> EX 86400)    │
│  • Prometheus Metric Exporter (/metrics)                     │
└───────────────┬──────────────────────────────▲───────────────┘
                │                              │
     (Polled via SKIP LOCKED)                  │ (Exposes /metrics)
                ▼                              │
┌──────────────────────────────┐               │
│   Outbox Dispatcher Engine   │               │
│   (XADD jobs:stream MAXLEN~) │               │
└───────────────┬──────────────┘               │
                │                              │
                ▼                              │
┌──────────────────────────────────────────────────────────────┐
│                Redis Streams Fabric (7.4)                    │
│   Stream: `jobs:stream` | Consumer Group: `job_workers`      │
│   DLQ Stream: `jobs:dlq`                                     │
└───────┬──────────────────────────────────────────────┬───────┘
        │                                              │
(XREADGROUP >)                                 (XAUTOCLAIM min_idle=8s)
        ▼                                              ▼
┌──────────────────────────────┐               ┌──────────────────────────────┐
│    Worker Node A (Active)    │               │   Worker Node B (Recovery)   │
│   • Heartbeat: Redis TTL     │               │   • Steals idle PEL tasks    │
│   • Step 1: Parsing ──► DB   │               │   • Checks `workflow_steps`  │
│   • Step 2: Embed   ──► DB   │ ──(CRASH!)──► │   • Skips Steps 1 & 2        │
│   • Step 3: Index   ──► DB   │               │   • Resumes Step 3 ──► DB    │
│   • XACK jobs:stream         │               │   • XACK jobs:stream         │
└──────────────────────────────┘               └──────────────────────────────┘


---

## ⚡ Core Architectural Invariants

### 1. Dual-Write Avoidance (Transactional Outbox Pattern)
To guarantee consistency between PostgreSQL and Redis Streams without distributed transactions, API requests write to both the `jobs` table and an `outbox_events` table inside a single PostgreSQL database transaction. A background **Outbox Dispatcher** polls pending events using row locks (`SELECT ... FOR UPDATE SKIP LOCKED`), publishes messages to `jobs:stream` via `XADD`, and marks records as `published`.

### 2. Strict Two-Tier Idempotency
Clients pass an `X-Idempotency-Key` header with requests. 
* **Fast-Path (Redis):** `SET key PENDING NX EX 60` locks the key. Subsequent identical requests while processing return `HTTP 409 Conflict`.
* **Finalized State:** Once committed, the Redis key stores `<job_id>` with an 86,400-second TTL. Re-submissions return `HTTP 202 Accepted` along with existing job state and step outputs.

### 3. Fault-Tolerant Step Checkpointing & Recovery
Long-running AI workloads (parsing, embedding, indexing) checkpoint intermediate results into the `workflow_steps` table upon completing each step. If a worker crashes mid-pipeline:
1. A surviving worker claims the stranded task using `XAUTOCLAIM` (min idle age: 8 seconds).
2. The pipeline queries completed `workflow_steps` for that job ID.
3. Steps marked `succeeded` are skipped (`reason=already_checkpointed`), preventing duplicate compute and LLM/embedding API costs.

### 4. Poison Pill Mitigation & DLQ Routing
If a job causes worker crashes or continuous errors exceeding `JOB_MAX_RETRIES` (evaluated via `XPENDING` delivery counts), the worker acknowledges (`XACK`) the stream message, writes the payload to `jobs:dlq`, and updates the job status in PostgreSQL to `dead_lettered`.

---

## 📊 Prometheus Telemetry & Metrics

The system exposes metrics on `/metrics` via the `prometheus-client` exporter:

| Metric Name | Type | Description / Labels |
| :--- | :--- | :--- |
| `jobs_submitted_total` | Counter | Total submitted jobs (`workflow_type`). |
| `idempotency_hits_total` | Counter | Deduplicated requests served from cache (`action="cached_result"`). |
| `step_duration_seconds` | Histogram | Execution timing breakdown per pipeline step (`step_name`). |
| `outbox_events_published_total` | Counter | Total outbox events successfully dispatched to Redis Streams. |

---

## 🚀 Getting Started & Execution

### 1. Infrastructure Setup
Spin up the PostgreSQL and Redis containers:
```powershell
docker compose up -d