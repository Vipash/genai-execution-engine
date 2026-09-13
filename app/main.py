import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.router import api_router
from app.core.config import settings
from app.services.outbox_dispatcher import OutboxDispatcher

dispatcher_stop_event = asyncio.Event()
dispatcher_task: asyncio.Task | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global dispatcher_task
    # 1. Startup: Launch background Outbox Dispatcher
    dispatcher = OutboxDispatcher(poll_interval=0.5)
    dispatcher_task = asyncio.create_task(dispatcher.run(dispatcher_stop_event))
    
    yield
    
    # 2. Shutdown: Signal dispatcher to exit cleanly
    dispatcher_stop_event.set()
    if dispatcher_task:
        await dispatcher_task

app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(api_router, prefix="/v1")

@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy", "env": settings.APP_ENV}