import json
import logging
import sys
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Redis configuration
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL for caching, idempotency, and DND status",
        validation_alias="REDIS_URL"
    )

    # Database configuration
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/notification_db",
        description="Async SQLAlchemy database connection URL",
        validation_alias="DATABASE_URL"
    )

    # Kafka - High-Priority Cluster
    high_priority_kafka_servers: str = Field(
        default="localhost:9092",
        description="Bootstrap servers list for High-Priority Kafka cluster (comma separated)",
        validation_alias="HIGH_PRIORITY_KAFKA_BOOTSTRAP_SERVERS"
    )
    high_priority_kafka_security_protocol: str = Field(
        default="PLAINTEXT",
        description="Security protocol for High-Priority Kafka cluster",
        validation_alias="HIGH_PRIORITY_KAFKA_SECURITY_PROTOCOL"
    )
    high_priority_kafka_sasl_mechanism: Optional[str] = Field(
        default=None,
        description="SASL mechanism for High-Priority Kafka cluster authentication",
        validation_alias="HIGH_PRIORITY_KAFKA_SASL_MECHANISM"
    )
    high_priority_kafka_sasl_plain_username: Optional[str] = Field(
        default=None,
        description="SASL Plain username for High-Priority Kafka cluster authentication",
        validation_alias="HIGH_PRIORITY_KAFKA_SASL_PLAIN_USERNAME"
    )
    high_priority_kafka_sasl_plain_password: Optional[str] = Field(
        default=None,
        description="SASL Plain password for High-Priority Kafka cluster authentication",
        validation_alias="HIGH_PRIORITY_KAFKA_SASL_PLAIN_PASSWORD"
    )

    # Kafka - Low-Priority Cluster
    low_priority_kafka_servers: str = Field(
        default="localhost:9093",
        description="Bootstrap servers list for Low-Priority Kafka cluster (comma separated)",
        validation_alias="LOW_PRIORITY_KAFKA_BOOTSTRAP_SERVERS"
    )
    low_priority_kafka_security_protocol: str = Field(
        default="PLAINTEXT",
        description="Security protocol for Low-Priority Kafka cluster",
        validation_alias="LOW_PRIORITY_KAFKA_SECURITY_PROTOCOL"
    )
    low_priority_kafka_sasl_mechanism: Optional[str] = Field(
        default=None,
        description="SASL mechanism for Low-Priority Kafka cluster authentication",
        validation_alias="LOW_PRIORITY_KAFKA_SASL_MECHANISM"
    )
    low_priority_kafka_sasl_plain_username: Optional[str] = Field(
        default=None,
        description="SASL Plain username for Low-Priority Kafka cluster authentication",
        validation_alias="LOW_PRIORITY_KAFKA_SASL_PLAIN_USERNAME"
    )
    low_priority_kafka_sasl_plain_password: Optional[str] = Field(
        default=None,
        description="SASL Plain password for Low-Priority Kafka cluster authentication",
        validation_alias="LOW_PRIORITY_KAFKA_SASL_PLAIN_PASSWORD"
    )

    # Global Parameters (Deregistration/DND keys)
    dnd_redis_prefix: str = Field(
        default="dnd:",
        description="Redis key prefix for Do Not Disturb user records",
        validation_alias="DND_REDIS_PREFIX"
    )
    deregistration_redis_prefix: str = Field(
        default="deregistered:",
        description="Redis key prefix for deregistered user records",
        validation_alias="DEREGISTRATION_REDIS_PREFIX"
    )

    # Logging Configuration
    log_level: str = Field(
        default="INFO",
        description="Global log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
        validation_alias="LOG_LEVEL"
    )
    log_format: str = Field(
        default="JSON",
        description="Log formatting mode. Options: JSON, TEXT",
        validation_alias="LOG_FORMAT"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()

class JSONFormatter(logging.Formatter):
    """Custom formatter to generate structured logs in JSON format."""
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logging() -> None:
    """Initializes structured logging configuration globally."""
    root_logger = logging.getLogger()
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        
    handler = logging.StreamHandler(sys.stdout)
    
    if settings.log_format.upper() == "JSON":
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        )
        
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(settings.log_level.upper())
    
    # Suppress noise from dependencies
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
