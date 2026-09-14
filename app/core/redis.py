"""
Hardened Async Redis Client Connection Manager.
Includes explicit socket timeouts, reconnect retries, and keep-alive healthchecks.
"""
from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from app.core.config import settings

# Configure automatic retry with exponential backoff on connection drops
retry_strategy = Retry(ExponentialBackoff(cap=5, base=0.5), retries=3)

pool = ConnectionPool.from_url(
    settings.REDIS_URL,
    max_connections=50,
    decode_responses=True,
    # Network resilience settings
    socket_timeout=5.0,
    socket_connect_timeout=5.0,
    health_check_interval=15,  # Send PING every 15s to keep connection alive
    retry=retry_strategy,
    retry_on_timeout=True
)

def get_redis() -> Redis:
    return Redis(connection_pool=pool)