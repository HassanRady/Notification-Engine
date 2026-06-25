import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.domain.models import NotificationRequest, DeliveryReceipt
from app.api.middleware import verify_idempotency_key
from app.cache.redis_client import get_redis
from app.repository.database import get_db_session
from app.repository.notification import UserDndSetting
from app.cache.bloom_filter import check_user_dnd_status
from app.broker.router import NotificationRouter
from app.worker.batcher import status_batcher

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/v1/notify", status_code=status.HTTP_202_ACCEPTED)
async def ingest_notification(
    request: NotificationRequest,
    redis_client: Redis = Depends(get_redis),
    db_session: AsyncSession = Depends(get_db_session)
):
    """
    Ingests and routes a notification request.
    
    Processing steps:
      1. Apply idempotency deduplication. Duplicate requests instantly return 202 Accepted.
      2. Query user preferences (Bloom filter -> Redis cache -> Database).
      3. If user has active DND, skip routing and return a suppressed acknowledgment (202).
      4. If DND is inactive, resolve priority channel (Router) and publish to Kafka asynchronously.
      5. Return 202 Accepted on successful queuing.
    """
    # 1. Deduplication Check
    await verify_idempotency_key(request.idempotency_key, redis_client)

    # 2. Database Fallback Query Callback
    async def db_query_callback(uid: str) -> Optional[bool]:
        stmt = select(UserDndSetting.dnd_active).where(UserDndSetting.user_id == uid)
        result = await db_session.execute(stmt)
        return result.scalar_one_or_none()

    # 3. Resolve User DND Settings
    dnd_active = await check_user_dnd_status(
        user_id=request.user_id,
        redis_client=redis_client,
        db_fallback_query=db_query_callback
    )

    if dnd_active:
        logger.info(f"Suppressing notification {request.notification_id} for user {request.user_id} due to active DND settings.")
        return {
            "status": "SUPPRESSED",
            "message": "Notification skipped due to active DND settings.",
            "notification_id": request.notification_id
        }

    # 4. Route and Publish to Isolated Kafka Cluster
    try:
        kafka_client = NotificationRouter.route(request.priority)
        payload_bytes = request.model_dump_json().encode("utf-8")
        
        # Publish asynchronously to the isolated queue
        await kafka_client.send_message(
            topic="notifications",
            value=payload_bytes,
            key=request.user_id.encode("utf-8")
        )
    except Exception as e:
        logger.error(
            f"Failed to publish notification {request.notification_id} for user {request.user_id}: {e}", 
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue notification for delivery."
        )

    logger.info(f"Successfully ingested and routed notification {request.notification_id} for user {request.user_id}.")
    return {
        "status": "INGESTED",
        "message": "Notification successfully ingested and queued.",
        "notification_id": request.notification_id
    }

@router.post("/v1/webhook", status_code=status.HTTP_202_ACCEPTED)
async def delivery_webhook(receipt: DeliveryReceipt):
    """
    Callback webhook receiving delivery status receipt events.
    Appends the receipt immediately to the memory buffer of the StatusMicroBatcher
    and returns HTTP 202 Accepted.
    """
    receipt_data = {
        "notification_id": receipt.notification_id,
        "status": receipt.status,
        "sequence_id": receipt.sequence_id,
        "timestamp": receipt.timestamp
    }
    
    # Asynchronously append the receipt to the batcher buffer (non-blocking)
    await status_batcher.add(receipt_data)
    
    logger.debug(f"Webhook enqueued status receipt event for notification {receipt.notification_id}.")
    return {
        "status": "ACCEPTED",
        "message": "Receipt enqueued for processing.",
        "notification_id": receipt.notification_id
    }
