import json
import asyncio
import logging
from typing import List, Optional
from aiokafka import AIOKafkaConsumer
from app.worker.rate_limiter import RetryRouter, RateLimitException

logger = logging.getLogger(__name__)

class NotificationConsumerWorker:
    """
    Kafka consumer worker designed for high-performance non-blocking message processing.
    Continuously polls Kafka queues and coordinates retry routing and DLQ mechanisms.
    """
    def __init__(self, topics: List[str], bootstrap_servers: str, group_id: str) -> None:
        self.topics = topics
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Initializes and runs the Kafka consumer loop in the background."""
        if self._running:
            logger.warning(f"Consumer worker for topics {self.topics} is already active.")
            return

        logger.info(f"Starting consumer worker for topics: {self.topics} on {self.bootstrap_servers}...")
        try:
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            await self.consumer.start()
            self._running = True
            self._task = asyncio.create_task(self._consume_loop())
            logger.info(f"Consumer worker for topics {self.topics} successfully started.")
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer worker: {e}", exc_info=True)
            self.consumer = None
            raise

    async def _consume_loop(self) -> None:
        """Asynchronous execution loop that polls and dispatches messages."""
        while self._running:
            try:
                # Poll for batches with a 1-second timeout
                msg_set = await self.consumer.getmany(timeout_ms=1000)
                for tp, messages in msg_set.items():
                    for msg in messages:
                        await self.process_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}", exc_info=True)
                # Short delay to prevent rapid spinning in error loops
                await asyncio.sleep(1)

    async def process_message(self, message) -> None:
        """
        Decodes a Kafka record, parses its retry headers, and dispatches it downstream.
        
        Handles RateLimitExceptions by routing the payload to the next delayed retry topic.
        """
        retry_count = 0
        if message.headers:
            for key, val in message.headers:
                if key == "retry_count":
                    try:
                        retry_count = int(val.decode("utf-8"))
                    except ValueError:
                        retry_count = 0

        logger.info(
            f"Polled record from topic={message.topic} partition={message.partition} "
            f"offset={message.offset} | retry_count={retry_count} | key={message.key}"
        )

        try:
            # Parse notification payload
            payload = json.loads(message.value.decode("utf-8"))
            
            # Downstream provider delivery simulation
            # If payload specifies 'trigger_rate_limit': True, simulate downstream 429
            inner_payload = payload.get("payload", {})
            if isinstance(inner_payload, dict) and inner_payload.get("trigger_rate_limit") is True:
                raise RateLimitException("Downstream HTTP 429: Too Many Requests.")

            logger.info(f"Notification {payload.get('notification_id')} successfully dispatched downstream.")
        except RateLimitException as e:
            logger.warning(f"Provider rate-limit triggered for key {message.key}: {e}")
            try:
                await RetryRouter.route_throttle_retry(
                    message_bytes=message.value,
                    key=message.key,
                    current_retry_count=retry_count
                )
            except Exception as routing_err:
                logger.error(f"Failed to route retry message: {routing_err}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected message execution error: {e}", exc_info=True)
            # Route directly to DLQ on malformed payloads
            try:
                await RetryRouter.route_throttle_retry(
                    message_bytes=message.value,
                    key=message.key,
                    current_retry_count=3  # Exceeds max count, forces DLQ
                )
            except Exception as dlq_err:
                logger.error(f"Failed to push message directly to DLQ: {dlq_err}", exc_info=True)

    async def stop(self) -> None:
        """Halts the polling task and gracefully stops the Kafka consumer connection."""
        logger.info(f"Stopping consumer worker for topics: {self.topics}...")
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None

        if self.consumer:
            try:
                await self.consumer.stop()
            except Exception as e:
                logger.error(f"Error closing consumer connection: {e}", exc_info=True)
            finally:
                self.consumer = None
        logger.info(f"Consumer worker for topics {self.topics} stopped.")
