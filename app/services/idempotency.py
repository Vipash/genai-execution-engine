"""
Distributed Idempotency Manager.
Combines fast Redis key-value checks with PostgreSQL fallback.
"""
import uuid
from typing import Literal
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job

class IdempotencyService:
    def __init__(self, redis: Redis, db: AsyncSession):
        self.redis = redis
        self.db = db

    def _redis_key(self, key: str) -> str:
        return f"idempotency:{key}"

    async def reserve_or_get(
        self, idempotency_key: str
    ) -> tuple[Literal["NEW", "CONFLICT", "EXISTING"], uuid.UUID | None]:
        """
        Attempts to reserve an idempotency key.
        
        Returns:
            - ("NEW", None): Key reserved successfully; proceed with creation.
            - ("CONFLICT", None): Another request with this key is currently in flight.
            - ("EXISTING", job_id): Request already completed; return existing job.
        """
        redis_key = self._redis_key(idempotency_key)
        
        # 1. Fast path: Check Redis cache
        cached_val = await self.redis.get(redis_key)
        if cached_val:
            if cached_val == "PROCESSING":
                return "CONFLICT", None
            try:
                return "EXISTING", uuid.UUID(cached_val)
            except ValueError:
                pass

        # 2. Redis Miss: Check PostgreSQL fallback (for expired Redis TTLs)
        stmt = select(Job.id).where(Job.idempotency_key == idempotency_key)
        result = await self.db.execute(stmt)
        existing_job_id = result.scalar_one_or_none()

        if existing_job_id:
            # Re-seed Redis with 24-hour TTL
            await self.redis.set(redis_key, str(existing_job_id), ex=86400)
            return "EXISTING", existing_job_id

        # 3. Reserve lock with 60s TTL to protect against concurrent duplicate in-flight requests
        acquired = await self.redis.set(redis_key, "PROCESSING", nx=True, ex=60)
        if not acquired:
            return "CONFLICT", None

        return "NEW", None

    async def finalize(self, idempotency_key: str, job_id: uuid.UUID) -> None:
        """Promotes reservation to a 24-hour cached pointer to the committed job."""
        redis_key = self._redis_key(idempotency_key)
        await self.redis.set(redis_key, str(job_id), ex=86400)

    async def release(self, idempotency_key: str) -> None:
        """Cleans up lock in the event of an unhandled failure during creation."""
        redis_key = self._redis_key(idempotency_key)
        await self.redis.delete(redis_key)