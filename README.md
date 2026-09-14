# Distributed GenAI Workflow & Job Execution Platform

[![CI Pipeline](https://github.com/Vipash/genai-execution-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/Vipash/genai-execution-engine/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16%2Bpgvector-4169E1.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis 7.4 Streams](https://img.shields.io/badge/Redis-7.4%20Streams-DC382D.svg?logo=redis&logoColor=white)](https://redis.io/)
[![SQLAlchemy 2.0](https://img.shields.io/badge/SQLAlchemy-2.0%20Async-D71F00.svg)](https://www.sqlalchemy.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An asynchronous, event-driven background job orchestration platform engineered for compute-heavy, multi-stage GenAI workloads (Document Ingestion &rarr; Semantic Chunking &rarr; Vector Embedding Generation &rarr; `pgvector` HNSW Indexing).

Engineered to guarantee **zero duplicate inferences** under high-concurrency bursts, **self-healing worker recovery** without repeating completed pipeline stages, and **strict dual-write consistency** across transactional and streaming storage layers.

---

## Table of Contents
- [Architectural Highlights](#architectural-highlights)
- [System Topology](#system-topology)
- [State Machine & Resumption Lifecycle](#state-machine--resumption-lifecycle)
- [Empirically Verified Benchmarks](#empirically-verified-benchmarks)
- [Configuration & Distributed Invariants](#configuration--distributed-invariants)
- [Quickstart & Local Deployment](#quickstart--local-deployment)
- [API Specification](#api-specification)
- [Authentication & Security Boundary](#authentication--security-boundary)
- [System Design Trade-Offs](#system-design-trade-offs)
- [Known Limitations & Production Roadmap](#known-limitations--production-roadmap)
- [Repository Structure](#repository-structure)

---

## Architectural Highlights

* **Distributed Idempotency Layer:** Multi-tier idempotency combining sub-millisecond atomic Redis reservations (`SET key NX EX`) with a durable PostgreSQL state machine fallback, eliminating redundant LLM inferences on network retries.
* **Transactional Outbox & Anti-Entropy Sweeper:** Eliminates the distributed dual-write problem between PostgreSQL and Redis Streams. State transitions and stream payloads are committed atomically in PostgreSQL and dispatched via row-level locks (`SELECT ... FOR UPDATE SKIP LOCKED`). A background anti-entropy reconciliation loop automatically detects and re-enqueues stranded tasks caused by broker disconnects.
* **Stateful Step-Level Checkpointing:** Individual pipeline stages persist intermediate JSON outputs to PostgreSQL upon completion. If a worker process crashes mid-pipeline (e.g., during vector storage), the re-claiming worker reads committed checkpoints and resumes execution at the point of failure.
* **Self-Healing Consumer Groups:** Background workers pull from Redis Streams consumer groups (`XREADGROUP`). Worker liveness is tracked via ephemeral TTL heartbeats. Abandoned tasks in the Pending Entries List (PEL) are automatically reclaimed by healthy workers using Redis 6.2+ `XAUTOCLAIM`.
* **Poison-Pill Dead-Letter Queue (DLQ):** Messages exceeding max retry limits (`JOB_MAX_RETRIES = 3`) are safely evicted from active consumer groups, routed to a `jobs:dlq` stream, and recorded as `dead_lettered` in PostgreSQL.
* **Native `pgvector` Integration:** Generates 1536-dimensional normalized float vectors, materializes them in a PostgreSQL vector store, and exposes sub-millisecond Cosine Distance (`<=>`) vector similarity search endpoints.
* **OpenMetrics Telemetry:** Exposes an OpenMetrics scrape endpoint (`/metrics`) recording real-time queue depth gauges, step execution duration histograms, worker counts, and idempotency collision rates.

---

## System Topology

```mermaid
flowchart TD
    Client["Client / Upstream Ingestion Service"] -->|POST /v1/jobs<br/>X-Idempotency-Key| Gateway

    subgraph Gateway["FastAPI Gateway"]
        direction TB
        G1["1. Redis Idempotency Guard<br/>SET key PROCESSING NX EX 60"] --> G2["2. PostgreSQL ACID Tx<br/>INSERT jobs + outbox_events"]
        G2 --> G3["3. Finalize Idempotency<br/>SET key job_id EX 86400"]
        G3 --> G4["4. Return HTTP 202 Accepted"]
    end

    Gateway -->|Async Poller<br/>FOR UPDATE SKIP LOCKED| Dispatcher["Outbox Dispatcher Engine"]
    Dispatcher -->|XADD MAXLEN~ 100k| Stream[("Redis 7.4 Streams<br/>jobs:stream")]

    Stream -->|XREADGROUP >| Worker1["Worker Node 1 (Active)"]
    Stream -->|XAUTOCLAIM min_idle=8s| Worker2["Worker Node 2 (Failover)"]

    Worker1 -.->|Crash mid-flight| Worker2
    Worker1 -->|Checkpoints Steps 1 and 2| PG[("PostgreSQL 16 + pgvector")]
    Worker2 -->|Reads Checkpoints, Resumes Step 3| PG
    Worker2 -->|XACK| Stream
    Gateway -.->|Scrapes /metrics| Prometheus["Prometheus Collector"]
```

---

## State Machine & Resumption Lifecycle

```mermaid
stateDiagram-v2
    [*] --> QUEUED: Job Created + Outbox Event Committed
    QUEUED --> RUNNING: Worker claims via XREADGROUP (Attempt 1)

    state RUNNING {
        [*] --> Step1_Parsing: Execute & Checkpoint
        Step1_Parsing --> Step2_Embedding: Execute & Checkpoint
        Step2_Embedding --> Step3_Indexing: Execute & Persist pgvector
        Step3_Indexing --> [*]
    }

    RUNNING --> WORKER_CRASH: Process killed (OOM / SIGKILL)
    WORKER_CRASH --> RECLAIMED: Idle > 8s detected via XAUTOCLAIM
    RECLAIMED --> RUNNING: Skip completed steps, resume at failure point (Attempt 2)

    RUNNING --> SUCCEEDED: All steps complete + XACK
    RUNNING --> DEAD_LETTERED: Attempts > MAX_RETRIES (3) -> Route to jobs:dlq

    SUCCEEDED --> [*]
    DEAD_LETTERED --> [*]
```

---

## Empirically Verified Benchmarks

Stress-tested using an asynchronous load-generation client firing 100 burst submissions across 20 concurrent connections with an intentionally injected 20% duplicate idempotency key rate.

### Test Environment
* **Host Machine:** AMD Ryzen 7 / Intel Core i7 (8 cores, 16 threads), 16 GB RAM
* **OS & Runtime:** Windows 11 Pro with WSL2 (Ubuntu 22.04 LTS), Docker Desktop 4.34
* **Database & Broker:** PostgreSQL 16.2 (`pgvector:pg16`), Redis 7.4-alpine
* **Concurrency Configuration:** 20 concurrent async worker HTTP connections, Semaphore bounded

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

## Configuration & Distributed Invariants

All runtime behavior is controlled via environment variables and distributed system invariants:

### Environment Settings (`.env`)
| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `DATABASE_URL` | `postgresql+asyncpg://...` | Asynchronous SQLAlchemy connection string |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis Stream & cache broker endpoint |
| `APP_ENV` | `development` | Deployment environment (`development` / `production` / `test`) |
| `PORT` | `8000` | FastAPI gateway listening port |
| `DEBUG` | `true` | Enables SQLAlchemy SQL echo logging |
| `WORKER_CONCURRENCY` | `5` | In-flight async task limit per worker process |
| `JOB_MAX_RETRIES` | `3` | Max delivery attempts before moving a task to `jobs:dlq` |
| `OPENAI_API_KEY` | *(required, no default)* | Credential used by the embedding stage to call `text-embedding-3-small`. The pipeline's Embed step will fail without this set. |

> **Note:** if the embedding stage is backed by a different provider or a local model in your setup, update this row and the "Native `pgvector` Integration" description above accordingly — both should name the actual embedding source in use.

### Architectural Timing Invariants
| Mechanism | Invariant Value | Operational Purpose |
| :--- | :--- | :--- |
| **Idempotency In-Flight TTL** | `60 seconds` | Prevents race condition submissions while first request creates DB rows |
| **Idempotency Cache TTL** | `86,400 seconds (24h)` | Fast-path cache of completed `job_id` mappings in Redis |
| **Worker Heartbeat Interval** | `5 seconds` | Frequency of `SET worker:heartbeat:<id> "active"` keep-alives |
| **Worker Heartbeat TTL** | `15 seconds` | Auto-expires worker liveness if the container is killed |
| **XAUTOCLAIM Min-Idle** | `8,000 ms (8s)` | Time before a task in the PEL is deemed abandoned and claimed by peer |
| **Anti-Entropy Reconciler SLA** | `180 seconds (3m)` | Threshold after which a stale `queued` job is re-dispatched to Redis |

---

## Quickstart & Local Deployment

### 1. Clone & Spin up Multi-Node Cluster
Clone the repository and launch the full stack—PostgreSQL (`pgvector`), Redis 7, database migrations, the API Gateway, and two scaled background workers:

```bash
git clone https://github.com/Vipash/genai-execution-engine.git
cd genai-execution-engine

# Build and launch with 2 parallel consumer group workers
docker compose up -d --build --scale worker=2
```

Verify that all service containers are healthy:
```bash
docker compose ps
```
![Docker Compose Running Services](docs/assets/docker-compose-running.png)

### 2. Submit an Ingestion Job
Submit a document workflow with an explicit idempotency key:
```bash
curl -X POST "http://localhost:8000/v1/jobs/" \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: job-key-001" \
  -d '{
    "workflow_type": "document_ingestion",
    "payload": {"document_url": "https://arxiv.org/pdf/1706.03762"}
  }'
```
![Job Submission Success via Swagger UI](docs/assets/job-submission-success.png)

### 3. Verify Database Schema
Confirm all Alembic migrations executed successfully by checking the database tables via `psql`:

![Database Schema Verification](docs/assets/db-schema-verification.png)

### 4. Verify Job Execution & Results
Check the worker logs to confirm the job was claimed, processed step-by-step, and completed:

![Worker Logs Execution Success](docs/assets/worker-execution-success.png)

Then check the job's status, parsed chunks, and generated vector embeddings (`text-embedding-3-small`, 1536 dimensions) via the Swagger UI at `http://localhost:8000/docs`, or by fetching `GET /v1/jobs/{id}` directly:

![Job Result Verification](docs/assets/job-result-success.png)

### 5. Query Vector Similarity
Query the closest semantic document chunks via Cosine Distance (`<=>`) in PostgreSQL:
```bash
curl "http://localhost:8000/v1/jobs/<JOB_UUID>/similar-chunks?chunk_index=0"
```

### 6. Execute Load & Stress Test
Trigger the 100-request concurrent load generator inside the gateway container:
```bash
docker exec -it genai_api python benchmark.py
```

### 7. Running Automated Integration Tests
Run the test suite locally against test containers:
```bash
pytest -v
```

---

## API Specification

| Method | Endpoint | Description | Status Codes |
| :--- | :--- | :--- | :--- |
| `POST` | `/v1/jobs/` | Submits a GenAI workflow job with strict `X-Idempotency-Key` validation | `202 Accepted`, `409 Conflict` |
| `GET` | `/v1/jobs/{id}` | Fetches job status, attempt logs, and intermediate step checkpoints | `200 OK`, `404 Not Found` |
| `GET` | `/v1/jobs/{id}/similar-chunks` | Performs real-time `pgvector` Cosine Distance (`<=>`) similarity search | `200 OK`, `404 Not Found` |
| `GET` | `/metrics` | Exposes OpenMetrics-compliant Prometheus telemetry | `200 OK` |
| `GET` | `/health` | Application liveness and configuration healthcheck probe | `200 OK` |

---

## Authentication & Security Boundary

* **Current Boundary:** This service is architected as an **internal orchestration platform / private subnet worker mesh**. The API Gateway does not enforce end-user JWT authentication or API keys.
* **Production Integration Hook:** In a production topology, this service sits behind an API Gateway / Reverse Proxy (e.g., Kong, Traefik, or an AWS ALB) terminating TLS and validating auth tokens, injecting trusted caller metadata headers downstream.

---

## System Design Trade-Offs

#### 1. Redis Streams vs. Celery / RabbitMQ
* **Decision:** Used native Redis Streams consumer groups (`XREADGROUP`, `XAUTOCLAIM`, `XPENDING`).
* **Trade-off:** Celery abstracts away broker details, which makes checkpoint-level resumption and low-level PEL inspection complex to manage. Redis Streams provides explicit ownership tracking, granular claim boundaries, and sub-millisecond dispatching without external broker complexity.

#### 2. Transactional Outbox vs. Direct Stream Publish
* **Decision:** Implemented an Outbox table inside the primary PostgreSQL transaction coupled with an asynchronous dispatcher using `FOR UPDATE SKIP LOCKED`.
* **Trade-off:** Direct writes (`db.commit()` followed by `stream.add()`) introduce a fatal dual-write vulnerability during network partitions. The outbox pattern adds a minimal polling delay (~0.5s) in exchange for absolute at-least-once delivery guarantees.

#### 3. Step-Level Checkpointing vs. Whole-Job Retries
* **Decision:** Intermediate outputs are committed to `workflow_steps` after each pipeline stage.
* **Trade-off:** Adds database write overhead between stages, but prevents re-running computationally expensive LLM embeddings (Step 2) when a downstream stage (such as vector index write) fails.

---

## Known Limitations & Production Roadmap

- [ ] **Outbox Table Truncation / Partitioning:** Currently, dispatched outbox records remain marked `published`. In high-throughput production, an automated `pg_cron` partition drop or archive worker is recommended to bound table size.
- [ ] **Redis Stream Trimming Strategy:** Streams use approximate trimming (`MAXLEN ~ 100000`). For enterprise audit retention, stream events should be archived to S3 / cold storage before trimming.
- [ ] **Dynamic Worker Auto-Scaling:** Current worker scaling is managed via Docker Compose (`--scale worker=N`). A production deployment on Kubernetes would bind KEDA to `genai_queue_depth` from `/metrics` to trigger Horizontal Pod Autoscaling (HPA).

---

## Repository Structure

```text
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
│   │   └── redis.py             # Hardened connection pool with backoff & keep-alives
│   ├── models/                  # SQLAlchemy 2.0 Mapped models (Job, Outbox, Embedding)
│   ├── schemas/                 # Pydantic v2 request/response validation
│   ├── services/
│   │   ├── idempotency.py       # Distributed multi-tier idempotency service
│   │   └── outbox_dispatcher.py # Outbox publisher with Anti-Entropy sweep loop
│   └── worker/
│       ├── consumer.py          # Consumer group worker: XREADGROUP, XAUTOCLAIM, DLQ routing, and checkpoint resumption
│       ├── heartbeat.py         # Ephemeral Redis TTL worker heartbeats
│       ├── pipeline.py          # Checkpointed GenAI pipeline & pgvector store
│       └── supervisor.py        # (verify role against consumer.py before publishing — see note below)
├── tests/                       # Pytest asynchronous integration test suite
├── benchmark.py                 # High-concurrency load testing engine
├── submit_job.py                # Cross-platform CLI job dispatcher
├── run_worker.py                # Standalone background worker runner
├── docker-compose.yml           # Multi-node cluster orchestration
├── Dockerfile                   # Lean Python 3.12 multi-stage image
├── LICENSE                      # MIT Open Source License
└── requirements.txt             # Production dependency manifest
```