from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.router import api_router
from app.core.config import settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup actions (e.g. pool validation)
    yield
    # Graceful shutdown (e.g. close connection pools)

app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(api_router, prefix="/v1")

@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy", "env": settings.APP_ENV}
