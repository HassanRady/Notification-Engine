import asyncio
import logging
from typing import Optional, Any, Dict
from aiokafka import AIOKafkaProducer
from app.config import settings

logger = logging.getLogger(__name__)

class KafkaClusterClient:
    """
    Asynchronous client wrapper around AIOKafkaProducer.
    Exposes starting, stopping, and message publishing routines with built-in logging and error management.
    """
    def __init__(self, name: str, producer_config: Dict[str, Any]) -> None:
        self.name = name
        self.producer_config = producer_config
        self.producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        """Initializes and starts the underlying asynchronous Kafka producer client."""
        if self.producer is not None:
            logger.warning(f"Kafka client for {self.name} is already started.")
            return

        logger.info(f"Starting Kafka producer for cluster: {self.name}...")
        try:
            self.producer = AIOKafkaProducer(**self.producer_config)
            await self.producer.start()
            logger.info(f"Kafka producer for cluster {self.name} started successfully.")
        except Exception as e:
            logger.error(f"Failed to start Kafka producer for cluster {self.name}: {e}", exc_info=True)
            self.producer = None
            raise

    async def send_message(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[list] = None
    ) -> Any:
        """
        Asynchronously publishes a message payload to a Kafka topic.
        
        Args:
            topic: The target topic to send the message to.
            value: The message payload byte array.
            key: Optional message serialization routing key.
            headers: Optional message metadata headers.
            
        Returns:
            RecordMetadata details concerning the stored message.
            
        Raises:
            RuntimeError: If called before the client connection has been started.
        """
        if self.producer is None:
            msg = f"Kafka producer for cluster {self.name} is not running."
            logger.error(msg)
            raise RuntimeError(msg)

        try:
            # send returns a future, which we immediately await to verify broker acknowledgment.
            future = await self.producer.send(topic, value=value, key=key, headers=headers)
            metadata = await future
            logger.debug(
                f"Successfully sent message to {self.name} cluster topic '{topic}' | "
                f"partition={metadata.partition} offset={metadata.offset}"
            )
            return metadata
        except Exception as e:
            logger.error(
                f"Error publishing message to {self.name} cluster topic '{topic}': {e}", 
                exc_info=True
            )
            raise

    async def stop(self) -> None:
        """Gracefully disconnects and stops the Kafka producer client."""
        if self.producer is None:
            logger.warning(f"Kafka client for {self.name} is not active.")
            return

        logger.info(f"Stopping Kafka producer for cluster: {self.name}...")
        try:
            await self.producer.stop()
            logger.info(f"Kafka producer for cluster {self.name} stopped successfully.")
        except Exception as e:
            logger.error(f"Failed to shut down Kafka producer for cluster {self.name}: {e}", exc_info=True)
        finally:
            self.producer = None

class KafkaManager:
    """
    Coordinates lifespan and connection properties for independent High-Priority 
    and Low-Priority Kafka clusters, keeping clients isolated as defined in configuration settings.
    """
    def __init__(self) -> None:
        self.high_priority: Optional[KafkaClusterClient] = None
        self.low_priority: Optional[KafkaClusterClient] = None

    def init_clients(self) -> None:
        """Configures individual KafkaClusterClients utilizing settings parameters."""
        logger.info("Initializing independent High and Low Priority Kafka client configurations...")

        # Build configurations from settings helper
        hp_config = {
            "bootstrap_servers": settings.high_priority_kafka_servers,
            "security_protocol": settings.high_priority_kafka_security_protocol,
        }
        if settings.high_priority_kafka_sasl_mechanism:
            hp_config["sasl_mechanism"] = settings.high_priority_kafka_sasl_mechanism
        if settings.high_priority_kafka_sasl_plain_username:
            hp_config["sasl_plain_username"] = settings.high_priority_kafka_sasl_plain_username
        if settings.high_priority_kafka_sasl_plain_password:
            hp_config["sasl_plain_password"] = settings.high_priority_kafka_sasl_plain_password

        self.high_priority = KafkaClusterClient(name="High-Priority", producer_config=hp_config)

        lp_config = {
            "bootstrap_servers": settings.low_priority_kafka_servers,
            "security_protocol": settings.low_priority_kafka_security_protocol,
        }
        if settings.low_priority_kafka_sasl_mechanism:
            lp_config["sasl_mechanism"] = settings.low_priority_kafka_sasl_mechanism
        if settings.low_priority_kafka_sasl_plain_username:
            lp_config["sasl_plain_username"] = settings.low_priority_kafka_sasl_plain_username
        if settings.low_priority_kafka_sasl_plain_password:
            lp_config["sasl_plain_password"] = settings.low_priority_kafka_sasl_plain_password

        self.low_priority = KafkaClusterClient(name="Low-Priority", producer_config=lp_config)
        logger.info("Kafka cluster configuration mapping complete.")

    async def start(self) -> None:
        """Starts both High-Priority and Low-Priority cluster clients in parallel."""
        if not self.high_priority or not self.low_priority:
            msg = "Kafka clients must be initialized via init_clients() before they can be started."
            logger.error(msg)
            raise RuntimeError(msg)

        logger.info("Establishing connections to Kafka clusters...")
        await asyncio.gather(
            self.high_priority.start(),
            self.low_priority.start()
        )

    async def stop(self) -> None:
        """Teardown both cluster connections concurrently."""
        logger.info("Stopping all active Kafka cluster client connection tasks...")
        tasks = []
        if self.high_priority:
            tasks.append(self.high_priority.stop())
        if self.low_priority:
            tasks.append(self.low_priority.stop())

        if tasks:
            # return_exceptions=True prevents one failed stop from blocking other stops
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Kafka cluster connection teardowns finalized.")

# Global Kafka client manager singleton
kafka_manager = KafkaManager()
