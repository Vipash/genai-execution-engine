"""
High-Concurrency Load & Stress Testing Engine.
Simulates burst submissions, measuring API latency, throughput, and idempotency rejection.
"""

import asyncio
import time

import httpx

GATEWAY_URL = "http://localhost:8000/v1/jobs/"
TOTAL_REQUESTS = 100
CONCURRENCY_LIMIT = 20


async def send_job(
    client: httpx.AsyncClient, job_idx: int, idempotency_key: str
) -> dict:
    payload = {
        "workflow_type": "document_ingestion",
        "payload": {"document_url": f"https://arxiv.org/doc/{job_idx}"},
    }
    headers = {"Content-Type": "application/json", "X-Idempotency-Key": idempotency_key}

    start = time.perf_counter()
    try:
        resp = await client.post(
            GATEWAY_URL, json=payload, headers=headers, timeout=10.0
        )
        latency = (time.perf_counter() - start) * 1000  # ms
        return {
            "status_code": resp.status_code,
            "latency": latency,
            "success": resp.status_code == 202,
        }
    except Exception as e: # noqa: BLE001
        return {"status_code": 0, "latency": 0, "success": False, "error": str(e)}


async def main():
    print(
        f"🚀 Launching Stress Test: {TOTAL_REQUESTS} requests (Concurrency: {CONCURRENCY_LIMIT})..."
    )

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async def sem_task(client, idx, key):
        async with semaphore:
            return await send_job(client, idx, key)

    # 1. Generate keys (intentionally duplicate 20% to test concurrent idempotency guards)
    keys = [f"bench-job-{i}" for i in range(int(TOTAL_REQUESTS * 0.8))]
    while len(keys) < TOTAL_REQUESTS:
        keys.append(keys[len(keys) % int(TOTAL_REQUESTS * 0.8)])  # Add duplicate keys

    overall_start = time.perf_counter()

    async with httpx.AsyncClient() as client:
        tasks = [sem_task(client, i, keys[i]) for i in range(TOTAL_REQUESTS)]
        results = await asyncio.gather(*tasks)

    overall_duration = time.perf_counter() - overall_start

    latencies = [r["latency"] for r in results if r["success"]]
    successes = sum(1 for r in results if r["success"])
    conflicts = sum(1 for r in results if r["status_code"] in (200, 409))
    failures = TOTAL_REQUESTS - successes - conflicts

    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)] if latencies else 0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0

    print("\n" + "=" * 50)
    print("           BENCHMARK RESULTS SUMMARY              ")
    print("=" * 50)
    print(f"Total Requests Dispatched:    {TOTAL_REQUESTS}")
    print(f"Total Time Taken:             {overall_duration:.2f} seconds")
    print(
        f"Throughput:                   {TOTAL_REQUESTS / overall_duration:.2f} req/s"
    )
    print(f"Successfully Created (202):   {successes}")
    print(f"Idempotency Cache Hits:       {conflicts}")
    print(f"Errors / Failures:            {failures}")
    print("-" * 50)
    print(f"Latency P50:                  {p50:.2f} ms")
    print(f"Latency P95:                  {p95:.2f} ms")
    print(f"Latency P99:                  {p99:.2f} ms")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
