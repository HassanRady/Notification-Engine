import pytest
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.repository.notification import (
    NotificationState, 
    bulk_upsert_notification_states
)
from app.worker.rate_limiter import RetryRouter
from app.broker.kafka_client import kafka_manager
from app.worker.batcher import StatusMicroBatcher
from app.api.main import app
from app.cache.redis_client import get_redis
from app.repository.database import get_db_session

# --- Unit Test 1: Bulk Upserts & OCC Dialect-Specific Checks ---
@pytest.mark.asyncio
async def test_bulk_upsert_postgres_compilation():
    mock_session = AsyncMock()
    mock_session.bind.dialect.name = "postgresql"
    
    mock_context = AsyncMock()
    mock_context.__aenter__.return_value = mock_session
    mock_session_maker = MagicMock(return_value=mock_context)

    with patch("app.repository.notification.async_session_maker", mock_session_maker):
        receipts = [
            {
                "notification_id": "notif_occ_pg",
                "status": "SENT",
                "sequence_id": 10,
                "timestamp": datetime.now(timezone.utc)
            }
        ]
        await bulk_upsert_notification_states(receipts)
        
        # Verify database call was made
        mock_session.execute.assert_called_once()
        
        # Check generated statement compiled to PG upsert syntax
        stmt = mock_session.execute.call_args[0][0]
        # Compile statement with PG dialect check
        from sqlalchemy.dialects.postgresql import dialect as pg_dialect
        compiled_sql = str(stmt.compile(dialect=pg_dialect()))
        
        assert "INSERT INTO notification_states" in compiled_sql
        assert "ON CONFLICT" in compiled_sql
        assert "DO UPDATE" in compiled_sql
        assert "sequence_id > notification_states.sequence_id" in compiled_sql

@pytest.mark.asyncio
async def test_bulk_upsert_sqlite_compilation():
    mock_session = AsyncMock()
    mock_session.bind.dialect.name = "sqlite"
    
    mock_context = AsyncMock()
    mock_context.__aenter__.return_value = mock_session
    mock_session_maker = MagicMock(return_value=mock_context)

    with patch("app.repository.notification.async_session_maker", mock_session_maker):
        receipts = [
            {
                "notification_id": "notif_occ_sqlite",
                "status": "SENT",
                "sequence_id": 5,
                "timestamp": datetime.now(timezone.utc)
            }
        ]
        await bulk_upsert_notification_states(receipts)
        
        mock_session.execute.assert_called_once()
        
        stmt = mock_session.execute.call_args[0][0]
        from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
        compiled_sql = str(stmt.compile(dialect=sqlite_dialect()))
        
        assert "INSERT INTO notification_states" in compiled_sql
        assert "ON CONFLICT" in compiled_sql
        assert "DO UPDATE" in compiled_sql

# --- Unit Test 2: Rate Limiter Exponential Backoff & DLQ ---
@pytest.mark.asyncio
async def test_retry_router_routing():
    mock_lp_client = AsyncMock()
    kafka_manager.low_priority = mock_lp_client

    message_payload = b"test_message"
    key = b"user_key"

    # Attempt 1: Should route to retry_1m
    topic, delay = await RetryRouter.route_throttle_retry(message_payload, key, 0)
    assert topic == "retry_1m"
    assert 48 <= delay <= 72  # 60s base delay +/- 20% jitter
    mock_lp_client.send_message.assert_called_once()
    
    # Attempt 2: Should route to retry_5m
    mock_lp_client.reset_mock()
    topic, delay = await RetryRouter.route_throttle_retry(message_payload, key, 1)
    assert topic == "retry_5m"
    assert 240 <= delay <= 360  # 300s base delay +/- 20% jitter

    # Attempt 4 (>3 attempts): Should route to DLQ
    mock_lp_client.reset_mock()
    topic, delay = await RetryRouter.route_throttle_retry(message_payload, key, 3)
    assert topic == "notifications_dlq"
    assert delay == 0
    mock_lp_client.send_message.assert_called_once_with(
        topic="notifications_dlq",
        value=message_payload,
        key=key,
        headers=[("retry_count", b"3")]
    )

# --- Unit Test 3: Micro-Batcher Buffering ---
@pytest.mark.asyncio
async def test_micro_batcher_size_flushing():
    # Setup batcher with size threshold 3
    batcher = StatusMicroBatcher(batch_size=3, flush_interval_ms=500)
    
    mock_upsert = AsyncMock()
    with patch("app.worker.batcher.bulk_upsert_notification_states", mock_upsert):
        batcher.start()
        
        # Add 1 item -> no flush
        await batcher.add({"notification_id": "n1", "status": "SENT", "sequence_id": 1})
        await asyncio.sleep(0.05)
        mock_upsert.assert_not_called()

        # Add 2 more items -> triggers flush immediately (total 3)
        await batcher.add({"notification_id": "n2", "status": "SENT", "sequence_id": 2})
        await batcher.add({"notification_id": "n3", "status": "SENT", "sequence_id": 3})
        
        # Give asyncio loop a tick to process
        await asyncio.sleep(0.05)
        mock_upsert.assert_called_once()
        assert batcher.queue.empty() is True
        
        await batcher.stop()

@pytest.mark.asyncio
async def test_micro_batcher_timeout_flushing():
    # Setup batcher with size 100, interval 50ms
    batcher = StatusMicroBatcher(batch_size=100, flush_interval_ms=50)
    
    mock_upsert = AsyncMock()
    with patch("app.worker.batcher.bulk_upsert_notification_states", mock_upsert):
        batcher.start()
        
        await batcher.add({"notification_id": "n1", "status": "SENT", "sequence_id": 1})
        mock_upsert.assert_not_called()
        
        # Sleep past the 50ms threshold
        await asyncio.sleep(0.1)
        mock_upsert.assert_called_once()
        
        await batcher.stop()

# --- Unit Test 4: Webhook Handler & API Integrations ---
def test_webhook_ingestion_api():
    mock_redis = AsyncMock()
    mock_db = AsyncMock()
    
    app.dependency_overrides[get_redis] = lambda: mock_redis
    app.dependency_overrides[get_db_session] = lambda: mock_db

    # Patch batcher to avoid actual write attempts during API checks
    mock_batcher_add = AsyncMock()
    with patch("app.api.handlers.status_batcher.add", mock_batcher_add):
        client = TestClient(app)
        
        webhook_payload = {
            "notification_id": "notif_webhook_1",
            "status": "DELIVERED",
            "sequence_id": 5,
            "timestamp": "2026-06-24T12:00:00Z"
        }
        
        response = client.post("/v1/webhook", json=webhook_payload)
        assert response.status_code == 202
        assert response.json()["status"] == "ACCEPTED"
        
        mock_batcher_add.assert_called_once()

    app.dependency_overrides.clear()
