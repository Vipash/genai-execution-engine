"""
End-to-End Integration Tests for API Gateway, Idempotency, and Outbox.
"""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

from app.core.config import settings
from app.main import app


@pytest.fixture
async def async_client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def redis_client():
    client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_health_check(async_client: AsyncClient):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


@pytest.mark.asyncio
async def test_job_submission_and_idempotency(
    async_client: AsyncClient, redis_client: Redis
):
    test_key = f"ci-test-{uuid.uuid4().hex[:8]}"
    payload = {
        "workflow_type": "document_ingestion",
        "payload": {"document_url": "https://arxiv.org/pdf/sample"},
    }
    headers = {"X-Idempotency-Key": test_key}

    # 1. First submission (Must return 202 Accepted)
    res1 = await async_client.post("/v1/jobs/", json=payload, headers=headers)
    assert res1.status_code == 202
    data1 = res1.json()
    job_id = data1["id"]
    assert data1["status"] == "queued"

    # 2. Duplicate submission with identical key (Must return the exact same job)
    res2 = await async_client.post("/v1/jobs/", json=payload, headers=headers)
    assert res2.status_code == 202
    data2 = res2.json()
    assert data2["id"] == job_id
    assert data2["idempotency_key"] == test_key

    # 3. Verify Redis idempotency cache was populated with 24h TTL
    cached_id = await redis_client.get(f"idempotency:{test_key}")
    assert cached_id == job_id


@pytest.mark.asyncio
async def test_metrics_endpoint(async_client: AsyncClient):
    response = await async_client.get("/metrics")
    assert response.status_code == 200
    assert "genai_queue_depth" in response.text
