from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.config import setup_logging
from app.api.handlers import router as api_router
from app.cache.redis_client import redis_manager
from app.broker.kafka_client import kafka_manager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages start-up initialization and clean shutdown of caching and queue clients."""
    # 1. Initialize structured logging
    setup_logging()
    
    # 2. Start Redis Connection Pool
    redis_manager.init_pool()
    
    # 3. Configure Kafka cluster producer clients
    kafka_manager.init_clients()
    
    yield
    
    # 4. Gracefully close Redis connections
    await redis_manager.close()
    
    # 5. Gracefully disconnect Kafka producers
    await kafka_manager.stop()

app = FastAPI(
    title="Notification Engine Service",
    description="Asynchronous, high-throughput ingestion and routing engine.",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(api_router)
