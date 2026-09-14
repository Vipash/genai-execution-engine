# Distributed GenAI Workflow & Job Execution Platform

[![CI Pipeline](https://github.com/yourusername/genai-execution-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/yourusername/genai-execution-engine/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16%2Bpgvector-4169E1.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis 7.4 Streams](https://img.shields.io/badge/Redis-7.4%20Streams-DC382D.svg?logo=redis&logoColor=white)](https://redis.io/)
[![SQLAlchemy 2.0](https://img.shields.io/badge/SQLAlchemy-2.0%20Async-D71F00.svg)](https://www.sqlalchemy.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An asynchronous, event-driven background job orchestration platform engineered for compute-heavy, multi-stage GenAI workloads (Document Ingestion &rarr; Semantic Chunking &rarr; Vector Embedding Generation &rarr; `pgvector` HNSW Indexing).

Engineered to guarantee **zero duplicate inferences** under high-concurrency bursts, **self-healing worker recovery** without repeating already completed pipeline steps, and **strict dual-write consistency** across transactional and streaming storage layers.

---

## Architectural Highlights

* **Distributed Idempotency Layer:** Multi-tier idempotency combining sub-millisecond atomic Redis reservations (`SET key NX EX`) with a durable PostgreSQL state machine fallback, preventing duplicate inference tasks on network timeouts.
* **Transactional Outbox & Anti-Entropy Sweeper:** Eliminates the dual-write problem between PostgreSQL and Redis Streams. State transitions and stream payloads are committed atomically in PostgreSQL and dispatched via row-level locks (`SELECT ... FOR UPDATE SKIP LOCKED`). A background anti-entropy reconciliation loop automatically detects and re-enqueues stranded tasks caused by broker disconnects.
* **Stateful Step-Level Checkpointing:** Individual pipeline stages persist their intermediate JSON outputs to PostgreSQL upon completion. If a worker node crashes mid-pipeline (e.g., during vector storage), the re-claiming worker reads committed checkpoints and resumes execution at the point of failure.
* **Self-Healing Consumer Groups:** Background workers pull from Redis Streams consumer groups (`XREADGROUP`). Worker health is tracked via ephemeral TTL heartbeats. Abandoned tasks in the Pending Entries List (PEL) are automatically reclaimed by healthy workers using Redis 6.2+ `XAUTOCLAIM`.
* **Poison-Pill Dead-Letter Queue (DLQ):** Messages exceeding max retry limits (`JOB_MAX_RETRIES = 3`) are safely evicted from active consumer groups, routed to a `jobs:dlq` stream, and recorded as `dead_lettered` in PostgreSQL.
* **Native `pgvector` Integration:** Generates 1536-dimensional normalized float vectors, materializes them in a PostgreSQL vector store, and exposes sub-millisecond Cosine Distance (`<=>`) vector similarity search endpoints.
* **OpenMetrics Telemetry:** Exposes an OpenMetrics scrape endpoint (`/metrics`) recording real-time queue depth gauges, step execution duration histograms, worker counts, and idempotency collision rates.

---

## System Architecture

                                  PLATFORM SYSTEM TOPOLOGY

[ Client / Upstream Ingestion Service ] │ ▼ (POST /v1/jobs with
X-Idempotency-Key)
┌────────────────────────────────────────────────────────────────────────┐
│ FastAPI Gateway │ │ 1. Fast-Path Redis Idempotency Check: │ │ • If exists as
UUID -> Return existing job (200 OK) │ │ • If exists as "PROCESSING" ->
Return 409 Conflict │ │ • If absent -> SET key "PROCESSING" NX EX 60 │ │ 2.
Atomic PostgreSQL Transaction: │ │ • INSERT INTO jobs (status = 'queued') │ │ •
INSERT INTO outbox_events (status = 'pending') │ │ 3. Finalize Idempotency: SET
key <job_id> EX 86400 (24h TTL) │ │ 4. Return 202 Accepted │
└────────────────────┬───────────────────────────────────▲───────────────┘
│ │ (FOR UPDATE SKIP LOCKED) │ (Scrapes /metrics) ▼ │
┌────────────────────────────────────────┐
│ │ Outbox Dispatcher Engine │ │ │ • Reads pending events │ │ │ • XADD to
jobs:stream (MAXLEN~ 100k) │ │ │ • Marks outbox status = 'published' │ │ │ •
Periodic Anti-Entropy Sweeper Loop │ │
└────────────────────┬───────────────────┘
│ │ │ ▼ │
┌────────────────────────────────────────────────────────┴───────────────┐
│ Redis 7.4 Streams Broker │ │ Stream: jobs:stream Consumer Group: job_workers │
│ Stream: jobs:dlq Worker Heartbeats: worker:heartbeat:* │
└───────────┬────────────────────────────────────────────────┬───────────┘
│ │ (XREADGROUP >) (XAUTOCLAIM min_idle=8s) ▼ ▼ ┌──────────────────────────────┐
┌──────────────────────────────┐ │ Worker Node 1 (Active) │ │ Worker Node 2
(Failover) │ │ • Pings Heartbeat (TTL: 15s) │ │ • Steals abandoned PEL tasks │ │
• Step 1: Parse ──► DB │ │ • Queries workflow_steps │ │ • Step 2: Embed ──► DB │
──(CRASHES!)──► │ • Skips Steps 1 & 2 │ │ • Step 3: pgvector ──► DB │ │ •
Resumes Step 3 ──► DB │ │ • XACK to consumer group │ │ • XACK to consumer group
│ └──────────────────────────────┘ └──────────────────────────────┘


---

## Empirically Verified Benchmarks

Stress-tested via an asynchronous load generator firing 100 burst submissions across 20 concurrent connections with an intentionally injected 20% duplicate idempotency key rate:

| Metric | Single Worker (Host) | Scaled Cluster (`--scale worker=2`) | Performance Delta |
| :--- | :--- | :--- | :--- |
| **Total Requests Dispatched** | 100 | 100 | &mdash; |
| **Ingestion Time Window** | 4.49 seconds | **2.38 seconds** | **~47% reduction** |
| **Gateway Throughput** | 22.27 req/sec | **42.08 req/sec** | **~1.9x throughput** |
| **Duplicate Keys Intercepted** | 20 (100% collision catch) | 20 (100% collision catch) | Zero double-inferences |
| **Failed / Dropped Requests** | **0** | **0** | **100% durability** |
| **Gateway Latency (P50)** | 405.98 ms | **283.06 ms** | **30% faster** |
| **Gateway Latency (P95)** | 2,412.78 ms | **1,043.93 ms** | **57% faster** |
| **Gateway Latency (P99)** | 2,452.28 ms | **1,106.28 ms** | **55% faster** |

---

## Crash-Recovery & Checkpoint Resumption Lifecycle

       ┌──────────┐
       │  QUEUED  │  Atomic DB commit + Outbox publication
       └────┬─────┘
            │
            ▼
       ┌──────────┐
       │ RUNNING  │  Worker claims task, updates job_attempts
       └────┬─────┘
            │

┌────────────┴────────────┐ ▼ ▼ [Step Checkpoint Loop] [Worker Process Crashes]
├─ Step 1: Parse │ ├─ Step 2: Embed ▼ (XAUTOCLAIM detects idle PEL > 8s) └─
Step 3: Index [Reclaimed by Healthy Worker Node] │ │ (Queries completed steps in
DB) ▼ ▼ ┌──────────┐ [Skips Steps 1 & 2, Resumes at Step 3] │SUCCEEDED │ │
└──────────┘ ▼ ┌──────────┐ │SUCCEEDED │ └──────────┘


---

## Quickstart & Local Deployment

### 1. Prerequisites
* [Docker](https://docs.docker.com/get-docker/) & Docker Compose
* Python 3.12+ (if developing locally)

### 2. Launch the Multi-Node Cluster
Clone the repository and bring up PostgreSQL (`pgvector`), Redis, Alembic migration runner, FastAPI Gateway, and two scaled workers:

```bash
git clone https://github.com/yourusername/genai-execution-engine.git
cd genai-execution-engine

# Build and launch with 2 parallel consumer group workers
docker compose up -d --build --scale worker=2

Verify that all service containers are healthy:

docker compose ps

3. Submit an Ingestion Job

Submit a job using the included Python client:

python submit_job.py my-first-job

Or via curl:

curl -X POST "http://localhost:8000/v1/jobs/" \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: job-key-001" \
  -d '{"workflow_type": "document_ingestion", "payload": {"document_url": "https://arxiv.org/pdf/1706.03762"}}'

4. Execute the Concurrency Benchmark

Trigger the load-testing suite against the gateway:

docker exec -it genai_api python benchmark.py

5. Query Vector Similarity

Perform a cosine similarity vector search over the indexed document chunks:

curl "http://localhost:8000/v1/jobs/<JOB_UUID>/similar-chunks?chunk_index=0"

API Specification

| Method | Endpoint                       | Description                                                               |
| :----- | :----------------------------- | :------------------------------------------------------------------------ |
| `POST` | `/v1/jobs/`                    | Submits a GenAI workflow job with strict `X-Idempotency-Key` validation.  |
| `GET`  | `/v1/jobs/{id}`                | Fetches detailed job status, attempts, and intermediate step checkpoints. |
| `GET`  | `/v1/jobs/{id}/similar-chunks` | Performs real-time `pgvector` Cosine Distance (`<=>`) similarity search.  |
| `GET`  | `/metrics`                     | Exposes OpenMetrics-compliant Prometheus telemetry.                       |
| `GET`  | `/health`                      | Liveness and configuration probe.                                         |

Production Invariants & Design Trade-Offs

1. Why Redis Streams over Celery or RabbitMQ?

Celery relies on opaque broker abstractions, making step-level database
checkpointing and custom re-claim loops difficult to orchestrate. Redis Streams
consumer groups (XREADGROUP, XAUTOCLAIM, XPENDING) grant direct, low-latency
control over message acknowledgments and consumer failover.

2. Why the Transactional Outbox Pattern?

Standard distributed backends suffer from the dual-write problem: if a database
write succeeds but the network fails before publishing to Redis, data
desynchronization is guaranteed. Storing events in an outbox_events table within
the same ACID database transaction and dispatching via FOR UPDATE SKIP LOCKED
guarantees at-least-once message delivery.

3. How does the Anti-Entropy Reconciler resolve edge cases?

If a network disconnect occurs immediately after XADD but before PostgreSQL
marks the outbox event as published, or if consumer group offsets skip an
unacknowledged message during a reset, an in-memory loop would lose the task.
The anti-entropy sweeper continuously scans for jobs remaining in queued past an
SLA threshold and re-enqueues them automatically.

Repository Structure

genai-execution-engine/
├── .github/
│   └── workflows/
│       └── ci.yml               # Automated linting, migration, and pytest workflow
├── alembic/                     # Async Alembic database migrations
├── app/
│   ├── api/v1/endpoints/jobs.py # Idempotent job submission & similarity search
│   ├── core/
│   │   ├── config.py            # Pydantic v2 BaseSettings
│   │   ├── db.py                # Async SQLAlchemy 2.0 engine & session factory
│   │   ├── metrics.py           # Prometheus counters, gauges, histograms
│   │   └── redis.py             # Connection pool with exponential backoff & keep-alives
│   ├── models/                  # SQLAlchemy 2.0 Mapped models (Job, Outbox, Embedding)
│   ├── schemas/                 # Pydantic v2 request/response validation
│   ├── services/
│   │   ├── idempotency.py       # Distributed multi-tier idempotency service
│   │   └── outbox_dispatcher.py # Outbox publisher with Anti-Entropy sweep loop
│   └── worker/
│       ├── consumer.py          # Consumer group worker with self-PEL drain
│       ├── heartbeat.py         # Ephemeral Redis TTL worker heartbeats
│       ├── pipeline.py          # Checkpointed GenAI pipeline & pgvector store
│       └── supervisor.py        # XAUTOCLAIM recovery engine & DLQ router
├── tests/                       # Pytest asynchronous integration test suite
├── benchmark.py                 # High-concurrency load testing engine
├── submit_job.py                # Cross-platform CLI job dispatcher
├── run_worker.py                # Standalone background worker runner
├── docker-compose.yml           # Multi-node cluster orchestration
├── Dockerfile                   # Lean Python 3.12 multi-stage image
└── requirements.txt             # Production dependency manifest


---

### Step 3: Run the Local Test Suite & Push

Verify that your new test suite passes locally:

```powershell
pytest -v
