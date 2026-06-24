import logging
from app.domain.models import PriorityEnum
from app.broker.kafka_client import KafkaClusterClient, kafka_manager

logger = logging.getLogger(__name__)

class NotificationRouter:
    """
    Directs notification messages to the correct Kafka cluster client based on message priority.
    Enforces isolation to safeguard transactional processing lanes from marketing volume spikes.
    """

    @classmethod
    def route(cls, priority: PriorityEnum) -> KafkaClusterClient:
        """
        Maps the given PriorityEnum classification to its corresponding KafkaClusterClient.
        
        Args:
            priority: Priority designation (TRANSACTIONAL or MARKETING).
            
        Returns:
            KafkaClusterClient: The client wrapper instance configured for the target cluster.
            
        Raises:
            RuntimeError: If the designated cluster client has not been initialized.
            ValueError: If the priority value is unknown.
        """
        if priority == PriorityEnum.TRANSACTIONAL:
            client = kafka_manager.high_priority
            if client is None:
                msg = "High-Priority Kafka client is not initialized in kafka_manager."
                logger.error(msg)
                raise RuntimeError(msg)
            logger.debug("Routing transactional request to the High-Priority Kafka cluster.")
            return client

        elif priority == PriorityEnum.MARKETING:
            client = kafka_manager.low_priority
            if client is None:
                msg = "Low-Priority Kafka client is not initialized in kafka_manager."
                logger.error(msg)
                raise RuntimeError(msg)
            logger.debug("Routing marketing request to the Low-Priority Kafka cluster.")
            return client

        else:
            msg = f"Unknown priority type: {priority}"
            logger.error(msg)
            raise ValueError(msg)
