"""
CLI runner for the Outbox Dispatcher and Recovery & DLQ Supervisor.
"""
import asyncio
import signal
import structlog
from app.services.outbox_dispatcher import OutboxDispatcher
from app.worker.supervisor import RecoverySupervisor

logger = structlog.get_logger(__name__)


async def main():
    stop_event = asyncio.Event()

    # Initialize workers
    outbox_dispatcher = OutboxDispatcher(poll_interval=1.0, batch_size=50)
    recovery_supervisor = RecoverySupervisor(min_idle_ms=10_000, check_interval=3)

    # Attach shared stop event to recovery supervisor if supported
    if hasattr(recovery_supervisor, "stop_event"):
        recovery_supervisor.stop_event = stop_event

    def handle_shutdown_signal():
        logger.info("supervisor.shutdown_signal_received")
        stop_event.set()

    # Register OS signals for graceful shutdown (Unix/Windows loop safety)
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_shutdown_signal)
    except NotImplementedError:
        # Fallback for Windows event loop limitations with signal handlers
        pass

    logger.info("background_supervisor.starting")

    try:
        # Run both tasks concurrently
        await asyncio.gather(
            outbox_dispatcher.run(stop_event),
            recovery_supervisor.run(),
        )
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("background_supervisor.interrupted")
    finally:
        stop_event.set()
        logger.info("background_supervisor.stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass