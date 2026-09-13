"""
CLI runner for the Recovery & DLQ Supervisor.
"""
import asyncio
import structlog
from app.worker.supervisor import RecoverySupervisor

logger = structlog.get_logger(__name__)

async def main():
    supervisor = RecoverySupervisor(min_idle_ms=10_000, check_interval=3)
    
    try:
        await supervisor.run()
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("supervisor.shutdown_requested")
        supervisor.stop_event.set()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass