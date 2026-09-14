"""
Prometheus Metrics Registry and Exporters.
"""

from prometheus_client import Counter, Gauge, Histogram

# 1. Job counters
JOBS_SUBMITTED = Counter(
    "genai_jobs_submitted_total",
    "Total number of jobs received by API Gateway",
    ["workflow_type"],
)

IDEMPOTENCY_HITS = Counter(
    "genai_idempotency_hits_total",
    "Total number of duplicate requests caught by Idempotency layer",
    ["action"],  # 'cached_redis' or 'cached_db'
)

JOBS_COMPLETED = Counter(
    "genai_jobs_completed_total",
    "Total number of jobs completed by workers",
    ["workflow_type", "status"],  # 'succeeded' or 'failed'
)

# 2. Latency Histograms
JOB_DURATION = Histogram(
    "genai_job_duration_seconds",
    "Total end-to-end execution duration of jobs in seconds",
    ["workflow_type"],
    buckets=[0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

STEP_DURATION = Histogram(
    "genai_step_duration_seconds",
    "Execution duration per pipeline step in seconds",
    ["step_name"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
)

# 3. Gauges (Instantaneous State)
QUEUE_DEPTH = Gauge(
    "genai_queue_depth",
    "Current number of pending messages waiting in the Redis Stream",
    ["stream_name"],
)

ACTIVE_WORKERS = Gauge(
    "genai_active_workers",
    "Current count of active registered workers reporting heartbeats",
)
