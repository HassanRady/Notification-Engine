import pytest
from datetime import datetime
from pydantic import ValidationError
from app.config import settings, setup_logging
from app.domain.models import NotificationRequest, DeliveryReceipt, PriorityEnum, StatusEnum
from app.domain.state_machine import DeliveryStateMachine, InvalidStateTransition
from app.cache.redis_client import redis_manager
from app.broker.kafka_client import kafka_manager

# Test 1: Config and logging setup
def test_config_initialization():
    assert settings.redis_url is not None
    assert settings.database_url is not None
    assert settings.high_priority_kafka_servers is not None
    assert settings.low_priority_kafka_servers is not None
    assert settings.dnd_redis_prefix == "dnd:"
    assert settings.deregistration_redis_prefix == "deregistered:"
    
    # Verify setup_logging executes without error
    setup_logging()

# Test 2: Pydantic Domain Model Validations
def test_notification_request_validation():
    # Valid Request
    req = NotificationRequest(
        user_id="user_123",
        notification_id="notif_abc",
        idempotency_key="key_xyz",
        priority=PriorityEnum.TRANSACTIONAL,
        payload={"message": "hello"}
    )
    assert req.user_id == "user_123"
    
    # Invalid priority
    with pytest.raises(ValidationError):
        NotificationRequest(
            user_id="user_123",
            notification_id="notif_abc",
            idempotency_key="key_xyz",
            priority="INVALID_PRIORITY",  # type: ignore
            payload={"message": "hello"}
        )

    # Whitespace-only string fields
    with pytest.raises(ValidationError):
        NotificationRequest(
            user_id="   ",
            notification_id="notif_abc",
            idempotency_key="key_xyz",
            priority=PriorityEnum.TRANSACTIONAL,
            payload={"message": "hello"}
        )

def test_delivery_receipt_validation():
    receipt = DeliveryReceipt(
        notification_id="notif_abc",
        status=StatusEnum.INGESTED,
        sequence_id=42
    )
    assert receipt.notification_id == "notif_abc"
    assert receipt.status == StatusEnum.INGESTED
    assert receipt.sequence_id == 42
    assert isinstance(receipt.timestamp, datetime)

    # Negative sequence ID
    with pytest.raises(ValidationError):
        DeliveryReceipt(
            notification_id="notif_abc",
            status=StatusEnum.INGESTED,
            sequence_id=-1
        )

# Test 3: Delivery State Machine Validations
def test_state_machine_valid_transitions():
    # Start (None) -> INGESTED
    assert DeliveryStateMachine.transition(None, None, StatusEnum.INGESTED, 1) == StatusEnum.INGESTED

    # INGESTED -> SENT
    assert DeliveryStateMachine.transition(StatusEnum.INGESTED, 1, StatusEnum.SENT, 2) == StatusEnum.SENT

    # SENT -> DELIVERED
    assert DeliveryStateMachine.transition(StatusEnum.SENT, 2, StatusEnum.DELIVERED, 3) == StatusEnum.DELIVERED

    # SENT -> FAILED
    assert DeliveryStateMachine.transition(StatusEnum.SENT, 2, StatusEnum.FAILED, 4) == StatusEnum.FAILED

def test_state_machine_invalid_transitions():
    # Transition directly from INGESTED to DELIVERED/FAILED without SENT should fail
    with pytest.raises(InvalidStateTransition):
        DeliveryStateMachine.transition(StatusEnum.INGESTED, 1, StatusEnum.DELIVERED, 2)

    # Transition from DELIVERED (terminal state) to SENT should fail
    with pytest.raises(InvalidStateTransition):
        DeliveryStateMachine.transition(StatusEnum.DELIVERED, 3, StatusEnum.SENT, 4)

    # Sequence ID regressions
    with pytest.raises(InvalidStateTransition) as exc_info:
        DeliveryStateMachine.transition(StatusEnum.INGESTED, 10, StatusEnum.SENT, 5)
    assert "sequence_id" in str(exc_info.value)

    # Duplicate sequence ID
    with pytest.raises(InvalidStateTransition):
        DeliveryStateMachine.transition(StatusEnum.INGESTED, 10, StatusEnum.SENT, 10)

# Test 4: Redis Manager connection configurations
def test_redis_manager_initialization():
    redis_manager.pool = None
    redis_manager.client = None
    
    redis_manager.init_pool()
    assert redis_manager.pool is not None
    assert redis_manager.client is not None

# Test 5: Kafka Manager configurations
def test_kafka_manager_initialization():
    kafka_manager.high_priority = None
    kafka_manager.low_priority = None
    
    kafka_manager.init_clients()
    assert kafka_manager.high_priority is not None
    assert kafka_manager.low_priority is not None
    assert kafka_manager.high_priority.producer_config["bootstrap_servers"] == settings.high_priority_kafka_servers
    assert kafka_manager.low_priority.producer_config["bootstrap_servers"] == settings.low_priority_kafka_servers
