import logging
from typing import Optional
from redis.asyncio import Redis, ConnectionPool
from app.config import settings

logger = logging.getLogger(__name__)

class RedisClientManager:
    """
    Manages the lifecycle and connection pooling for the redis.asyncio client.
    Enables clean startup initialization and shutdown teardown matching FastAPI lifespans.
    """
    def __init__(self) -> None:
        self.pool: Optional[ConnectionPool] = None
        self.client: Optional[Redis] = None

    def init_pool(self) -> None:
        """
        Creates and configures the Redis connection pool.
        This is synchronous but the pool connects lazily during async requests.
        """
        if self.pool is not None:
            logger.warning("Redis connection pool is already initialized.")
            return

        logger.info(f"Initializing Redis connection pool using URL: {settings.redis_url}")
        try:
            # max_connections is configured to handle hyper-scalable concurrent request volumes.
            # decode_responses=True simplifies handling data by returning strings rather than bytes.
            self.pool = ConnectionPool.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=100
            )
            self.client = Redis(connection_pool=self.pool)
            logger.info("Redis connection pool and client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Redis connection pool: {e}", exc_info=True)
            raise

    async def close(self) -> None:
        """
        Awaits closure of any active client operations and releases pool resources.
        Should be called inside the FastAPI shutdown lifespan event.
        """
        logger.info("Initiating Redis client shutdown...")
        if self.client:
            try:
                await self.client.aclose()
                logger.info("Redis connection instance closed.")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}", exc_info=True)
            finally:
                self.client = None

        if self.pool:
            try:
                await self.pool.disconnect()
                logger.info("Redis connection pool disconnected.")
            except Exception as e:
                logger.error(f"Error disconnecting Redis connection pool: {e}", exc_info=True)
            finally:
                self.pool = None
        logger.info("Redis cleanup completed.")

# Singleton manager instance to expose to the application lifespan.
redis_manager = RedisClientManager()

async def get_redis() -> Redis:
    """
    FastAPI dependency helper to retrieve the active, running Redis client.
    
    Raises:
        RuntimeError: If accessed prior to calling redis_manager.init_pool().
    """
    if redis_manager.client is None:
        raise RuntimeError(
            "Redis client has not been initialized. "
            "Please call `redis_manager.init_pool()` within the FastAPI startup lifecycle."
        )
    return redis_manager.client
