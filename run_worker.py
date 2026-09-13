"""
CLI runner for the Distributed Stream Worker.
"""
import asyncio
import signal
import sys
import structlog
from app.worker.consumer import StreamWorker

logger = structlog.get_logger(__name__)

async def main():
    worker = StreamWorker()
    loop = asyncio.get_running_loop()

    # Graceful shutdown handler
    def shutdown():
        logger.info("worker.received_shutdown_signal")
        worker.stop_event.set()

    # Signal registration (Windows compatible fallback)
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown)
    else:
        # On Windows, KeyboardInterrupt is caught via try-except
        pass

    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("worker.keyboard_interrupt")
        shutdown()

if __name__ == "__main__":
    asyncio.run(main())