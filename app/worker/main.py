import asyncio
import logging
import signal
from typing import List
from app.config import setup_logging, settings
from app.cache.redis_client import redis_manager
from app.broker.kafka_client import kafka_manager
from app.worker.batcher import status_batcher
from app.worker.consumer import NotificationConsumerWorker

logger = logging.getLogger(__name__)

async def run_worker() -> None:
    """
    Main orchestrator for the Notification Engine worker process.
    
    Sets up connection pools, starts background batchers, spawns isolated consumer workers,
    and intercepts system termination signals to ensure zero message loss during shutdown.
    """
    # 1. Initialize structured logging
    setup_logging()
    logger.info("Initializing Notification Engine Worker Process...")

    # 2. Start global connections
    redis_manager.init_pool()
    kafka_manager.init_clients()
    
    # 3. Start memory buffer micro-batcher background loop
    status_batcher.start()

    # 4. Spawns isolated workers for traffic prioritization
    # High-Priority Consumer Worker (Transactional pipeline)
    hp_worker = NotificationConsumerWorker(
        topics=["notifications_high"],
        bootstrap_servers=settings.high_priority_kafka_servers,
        group_id="notification-engine-hp-group"
    )

    # Low-Priority Consumer Worker (Marketing queue + retry queues)
    lp_worker = NotificationConsumerWorker(
        topics=["notifications_low", "retry_1m", "retry_5m", "retry_15m"],
        bootstrap_servers=settings.low_priority_kafka_servers,
        group_id="notification-engine-lp-group"
    )

    # Start polling loops asynchronously
    await hp_worker.start()
    await lp_worker.start()

    # 5. Coordinate graceful shutdown on system termination signals
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("System signal caught. Initiating graceful worker shutdown sequence...")
        stop_event.set()

    # Attach listeners for Standard POSIX termination signals
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Fallback for systems (like Windows) where signal handlers are not fully supported
            pass

    # Await termination trigger
    await stop_event.wait()

    # 6. Graceful cleanup sequence (Fail Safe protocol)
    logger.info("Stopping polling loops and cleaning connections...")
    await hp_worker.stop()
    await lp_worker.stop()
    
    # Micro-batcher stop will force write remaining buffer elements
    await status_batcher.stop()
    
    await redis_manager.close()
    await kafka_manager.stop()
    logger.info("Worker process safely terminated.")

def main() -> None:
    """Synchronous entrypoint for running the worker process."""
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("Worker process interrupted by keyboard command.")

if __name__ == "__main__":
    main()
