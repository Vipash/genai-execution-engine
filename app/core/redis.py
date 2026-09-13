from redis.asyncio import ConnectionPool, Redis
from app.core.config import settings

pool = ConnectionPool.from_url(
    settings.REDIS_URL,
    max_connections=50,
    decode_responses=True
)

def get_redis() -> Redis:
    return Redis(connection_pool=pool)
