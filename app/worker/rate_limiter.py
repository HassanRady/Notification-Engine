import random
import logging
from typing import Optional, Tuple
from app.broker.kafka_client import kafka_manager

logger = logging.getLogger(__name__)

class RateLimitException(Exception):
    """Custom exception raised when encountering downstream provider HTTP 429 rate limits."""
    pass

class RetryRouter:
    """
    Manages downstream provider throttling by routing throttled payloads to delayed topics.
    
    Prevents event-loop blockages by delegating backoff logic to the broker under:
      - Attempt 1: retry_1m
      - Attempt 2: retry_5m
      - Attempt 3: retry_15m
      - Exceeded:  notifications_dlq (Dead Letter Queue)
    """
    RETRY_TOPICS = {
        1: "retry_1m",
        2: "retry_5m",
        3: "retry_15m"
    }
    DLQ_TOPIC = "notifications_dlq"

    @classmethod
    async def route_throttle_retry(
        cls,
        message_bytes: bytes,
        key: Optional[bytes],
        current_retry_count: int
    ) -> Tuple[str, int]:
        """
        Increments the retry count and publishes the message to the next time-delayed retry topic.
        If retry threshold is exceeded (> 3 attempts), the message is dispatched to the DLQ.
        
        Args:
            message_bytes: The original notification payload raw bytes.
            key: The routing/partition key.
            current_retry_count: The retry sequence number resolved from message headers.
            
        Returns:
            Tuple[str, int]: Resolved target topic name and scheduled backoff delay in seconds.
        """
        next_retry_count = current_retry_count + 1
        
        if next_retry_count > 3:
            logger.warning(
                f"Notification retry attempts exhausted ({current_retry_count}/3). "
                f"Routing payload to DLQ: '{cls.DLQ_TOPIC}'."
            )
            # Route to the Dead Letter Queue
            await kafka_manager.low_priority.send_message(
                topic=cls.DLQ_TOPIC,
                value=message_bytes,
                key=key,
                headers=[("retry_count", str(current_retry_count).encode("utf-8"))]
            )
            return cls.DLQ_TOPIC, 0

        target_topic = cls.RETRY_TOPICS.get(next_retry_count, "retry_1m")
        
        # Calculate exponential backoff base delay in seconds
        # Attempt 1: 1 min (60s), Attempt 2: 5 min (300s), Attempt 3: 15 min (900s)
        base_delay = 60 if next_retry_count == 1 else (300 if next_retry_count == 2 else 900)
        
        # Apply random jitter (+/- 20%) to avoid thundering herds
        jitter_range = base_delay * 0.2
        jitter = random.uniform(-jitter_range, jitter_range)
        total_delay = max(1, int(base_delay + jitter))

        logger.info(
            f"Routing throttled request to retry topic '{target_topic}' "
            f"| attempt={next_retry_count}/3 | backoff={total_delay}s."
        )

        # Publish the payload back to the Low-Priority cluster on the retry topic
        await kafka_manager.low_priority.send_message(
            topic=target_topic,
            value=message_bytes,
            key=key,
            headers=[
                ("retry_count", str(next_retry_count).encode("utf-8")),
                ("delay_seconds", str(total_delay).encode("utf-8"))
            ]
        )
        return target_topic, total_delay
