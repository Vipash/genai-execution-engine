"""
Worker Heartbeat Service.
Maintains ephemeral TTL keys in Redis indicating consumer liveness.
"""

import asyncio

import structlog
from redis.asyncio import Redis

logger = structlog.get_logger(__name__)


class HeartbeatService:
    def __init__(self, redis: Redis, worker_id: str, interval: int = 5, ttl: int = 15):
        self.redis = redis
        self.worker_id = worker_id
        self.interval = interval
        self.ttl = ttl
        self._key = f"worker:heartbeat:{worker_id}"

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("heartbeat.started", worker_id=self.worker_id)
        try:
            while not stop_event.is_set():
                await self.redis.set(self._key, "active", ex=self.ttl)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.interval)
                except asyncio.TimeoutError:
                    pass
        finally:
            # Clean up on graceful exit
            await self.redis.delete(self._key)
            logger.info("heartbeat.stopped", worker_id=self.worker_id)
