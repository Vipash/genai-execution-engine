"""
CLI runner for the Recovery & DLQ Supervisor.
"""
import asyncio
import structlog
from app.worker.supervisor import RecoverySupervisor

logger = structlog.get_logger(__name__)

async def main():
    # Checks every 5 seconds for messages idle > 15 seconds
    supervisor = RecoverySupervisor(min_idle_ms=15_000, check_interval=5)
    
    try:
        await supervisor.run()
    except KeyboardInterrupt:
        logger.info("supervisor.keyboard_interrupt")
        supervisor.stop_event.set()

if __name__ == "__main__":
    asyncio.run(main())