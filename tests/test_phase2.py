import pytest
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.cache.bloom_filter import BloomFilter, check_user_dnd_status
from app.broker.router import NotificationRouter
from app.broker.kafka_client import kafka_manager
from app.api.middleware import verify_idempotency_key
from app.api.main import app
from app.cache.redis_client import get_redis
from app.repository.database import get_db_session
from app.domain.models import PriorityEnum

# --- Unit Test 1: Bloom Filter ---
def test_bloom_filter_membership():
    bf = BloomFilter(expected_elements=1000, false_positive_rate=0.01)
    
    # Confirm definitive exclusion
    assert bf.contains("user_not_existing") is False
    
    # Add elements
    bf.add("user_opt_out_1")
    bf.add("user_opt_out_2")
    
    # Confirm inclusion check
    assert bf.contains("user_opt_out_1") is True
    assert bf.contains("user_opt_out_2") is True
    assert bf.contains("user_opt_out_3") is False

# --- Unit Test 2: Check DND Cache-Aside Logic ---
@pytest.mark.asyncio
async def test_check_user_dnd_status():
    from app.cache.bloom_filter import dnd_bloom_filter
    
    # Reset/clear dnd_bloom_filter bit_array for test isolation
    dnd_bloom_filter.bit_array = [False] * dnd_bloom_filter.size
    
    # Mock Redis client
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None
    
    # Mock DB query callback
    mock_db_query = AsyncMock(return_value=True)  # User has custom DND settings active
    
    # Case A: User NOT in Bloom Filter -> should skip Redis/DB checks
    user_not_in_bf = "user_default_settings"
    assert not dnd_bloom_filter.contains(user_not_in_bf)
    
    dnd_status = await check_user_dnd_status(user_not_in_bf, mock_redis, mock_db_query)
    assert dnd_status is False
    mock_redis.get.assert_not_called()
    mock_db_query.assert_not_called()

    # Case B: User IN Bloom Filter -> Cache Miss -> DB Hit -> Cache Write
    user_with_custom_dnd = "user_custom_settings"
    dnd_bloom_filter.add(user_with_custom_dnd)
    
    dnd_status = await check_user_dnd_status(user_with_custom_dnd, mock_redis, mock_db_query)
    assert dnd_status is True
    mock_redis.get.assert_called_once_with(f"dnd:{user_with_custom_dnd}")
    mock_db_query.assert_called_once_with(user_with_custom_dnd)
    mock_redis.set.assert_called_once_with(f"dnd:{user_with_custom_dnd}", "true", ex=3600)

    # Case C: User IN Bloom Filter -> Cache Hit (no DB query)
    mock_redis.reset_mock()
    mock_db_query.reset_mock()
    mock_redis.get.return_value = "true"
    
    dnd_status = await check_user_dnd_status(user_with_custom_dnd, mock_redis, mock_db_query)
    assert dnd_status is True
    mock_redis.get.assert_called_once_with(f"dnd:{user_with_custom_dnd}")
    mock_db_query.assert_not_called()

# --- Unit Test 3: Notification Routing ---
def test_notification_router():
    # Setup mock clients in kafka_manager
    mock_hp_client = MagicMock()
    mock_lp_client = MagicMock()
    
    kafka_manager.high_priority = mock_hp_client
    kafka_manager.low_priority = mock_lp_client

    assert NotificationRouter.route(PriorityEnum.TRANSACTIONAL) == mock_hp_client
    assert NotificationRouter.route(PriorityEnum.MARKETING) == mock_lp_client

    # Reset and test exception path
    kafka_manager.high_priority = None
    with pytest.raises(RuntimeError):
        NotificationRouter.route(PriorityEnum.TRANSACTIONAL)

# --- Unit Test 4: Idempotency Deduplication ---
@pytest.mark.asyncio
async def test_verify_idempotency_key_unique():
    mock_redis = AsyncMock()
    mock_redis.set.return_value = True
    
    await verify_idempotency_key("unique_key_1", mock_redis)
    mock_redis.set.assert_called_once_with("idempotency:unique_key_1", "1", nx=True, ex=86400)

@pytest.mark.asyncio
async def test_verify_idempotency_key_duplicate():
    mock_redis = AsyncMock()
    mock_redis.set.return_value = False
    
    with pytest.raises(HTTPException) as exc_info:
        await verify_idempotency_key("duplicate_key_1", mock_redis)
    assert exc_info.value.status_code == 202

# --- Unit Test 5: End-to-End API Ingestion (/v1/notify) ---
def test_api_ingestion_endpoint():
    # Override dependencies
    mock_redis = AsyncMock()
    mock_db = AsyncMock()
    
    # Deduplication set returns True (unique key)
    mock_redis.set.return_value = True
    
    # Mock Redis lookup for DND
    mock_redis.get.return_value = None  # Cache miss
    
    # Setup mock Kafka cluster clients
    mock_hp_client = AsyncMock()
    mock_lp_client = AsyncMock()
    kafka_manager.high_priority = mock_hp_client
    kafka_manager.low_priority = mock_lp_client

    app.dependency_overrides[get_redis] = lambda: mock_redis
    app.dependency_overrides[get_db_session] = lambda: mock_db

    client = TestClient(app)

    payload = {
        "user_id": "user_456",
        "notification_id": "notif_789",
        "idempotency_key": "idem_111",
        "priority": "TRANSACTIONAL",
        "payload": {"text": "Welcome to Phase 2!"}
    }

    # Case A: Success Ingestion
    response = client.post("/v1/notify", json=payload)
    assert response.status_code == 202
    assert response.json()["status"] == "INGESTED"
    
    mock_hp_client.send_message.assert_called_once()
    mock_lp_client.send_message.assert_not_called()

    # Case B: Duplicate Key (returns 202 with duplicate details)
    mock_redis.set.return_value = False  # Force duplicate
    response = client.post("/v1/notify", json=payload)
    assert response.status_code == 202
    assert "Duplicate" in response.json()["detail"]["message"]

    # Clean overrides
    app.dependency_overrides.clear()
