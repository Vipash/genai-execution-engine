import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.v1.router import api_router
from app.core.config import settings
from app.core.metrics import QUEUE_DEPTH
from app.core.redis import get_redis
from app.services.outbox_dispatcher import OutboxDispatcher

dispatcher_stop_event = asyncio.Event()
dispatcher_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global dispatcher_task
    dispatcher = OutboxDispatcher(poll_interval=0.5)
    dispatcher_task = asyncio.create_task(dispatcher.run(dispatcher_stop_event))
    yield
    dispatcher_stop_event.set()
    if dispatcher_task:
        await dispatcher_task


app = FastAPI(title=settings.APP_NAME, version="1.0.0", lifespan=lifespan)

app.include_router(api_router, prefix="/v1")


@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy", "env": settings.APP_ENV}


@app.get("/metrics", tags=["Observability"])
async def metrics():
    # Update current queue depth before scraping
    try:
        redis = get_redis()
        length = await redis.xlen("jobs:stream")
        QUEUE_DEPTH.labels(stream_name="jobs:stream").set(length)
        await redis.aclose()
    except Exception: # noqa: BLE001
        pass

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
