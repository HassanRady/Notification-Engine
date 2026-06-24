import logging
from fastapi import Header, Depends, HTTPException, status
from redis.asyncio import Redis
from app.cache.redis_client import get_redis

logger = logging.getLogger(__name__)

async def verify_idempotency_key(idempotency_key: str, redis_client: Redis) -> None:
    """
    Checks the uniqueness of an idempotency key atomically using Redis.
    
    If the key already exists (cache hit), it raises an HTTP 202 Accepted status 
    immediately, short-circuiting handler logic and avoiding duplicate runs.
    
    Args:
        idempotency_key: The unique request identifier token.
        redis_client: Active Redis client connection.
        
    Raises:
        HTTPException(202): On idempotency check hit (duplicate).
        HTTPException(500): On unexpected Redis connectivity errors (fail closed).
    """
    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or empty idempotency key."
        )

    redis_key = f"idempotency:{idempotency_key.strip()}"
    try:
        # SET key value NX EX 86400: Sets key to "1" only if it does not exist (NX),
        # with an expiration of 24 hours = 86400 seconds (EX).
        is_new_set = await redis_client.set(redis_key, "1", nx=True, ex=86400)
        
        if not is_new_set:
            logger.info(f"Duplicate request detected for idempotency key: {idempotency_key}. Returning HTTP 202.")
            raise HTTPException(
                status_code=status.HTTP_202_ACCEPTED,
                detail={
                    "status": "ACCEPTED",
                    "message": "Duplicate request acknowledged.",
                    "idempotency_key": idempotency_key
                }
            )
    except HTTPException:
        # Re-raise HTTPExceptions (i.e. the 202 Accepted redirect)
        raise
    except Exception as e:
        logger.error(f"Idempotency verification error for key {idempotency_key}: {e}", exc_info=True)
        # Fail closed under cache error states to protect down-stream services.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Deduplication check failure. Request rejected."
        )

async def require_idempotency_header(
    x_idempotency_key: str = Header(..., description="Unique request idempotency token"),
    redis_client: Redis = Depends(get_redis)
) -> str:
    """
    FastAPI dependency that extracts and validates the idempotency key from headers.
    Returns the key if it is unique.
    """
    await verify_idempotency_key(x_idempotency_key, redis_client)
    return x_idempotency_key
