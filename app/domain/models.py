from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any
from pydantic import BaseModel, Field, field_validator

class PriorityEnum(str, Enum):
    TRANSACTIONAL = "TRANSACTIONAL"
    MARKETING = "MARKETING"

class StatusEnum(str, Enum):
    INGESTED = "INGESTED"
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"

class NotificationRequest(BaseModel):
    user_id: str = Field(
        ..., 
        min_length=1, 
        description="Unique identifier of the recipient user"
    )
    notification_id: str = Field(
        ..., 
        min_length=1, 
        description="Unique system-wide identifier of the notification"
    )
    idempotency_key: str = Field(
        ..., 
        min_length=1, 
        description="Idempotency key to prevent duplicate processing"
    )
    priority: PriorityEnum = Field(
        ..., 
        description="Priority classification of the notification"
    )
    payload: Dict[str, Any] = Field(
        ..., 
        description="Arbitrary notification payload data"
    )

    @field_validator("user_id", "notification_id", "idempotency_key")
    @classmethod
    def validate_non_empty_whitespace(cls, v: str) -> str:
        """Ensures strings are not empty or solely whitespace, and strips them."""
        stripped = v.strip()
        if not stripped:
            raise ValueError("String field cannot be empty or consist only of whitespace.")
        return stripped

class DeliveryReceipt(BaseModel):
    notification_id: str = Field(
        ..., 
        min_length=1, 
        description="Associated notification identifier"
    )
    status: StatusEnum = Field(
        ..., 
        description="Delivery state of the notification"
    )
    sequence_id: int = Field(
        ..., 
        ge=0, 
        description="Monotonically increasing sequence number for state ordering"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when the delivery receipt was created"
    )

    @field_validator("notification_id")
    @classmethod
    def validate_non_empty_whitespace(cls, v: str) -> str:
        """Ensures notification_id is not empty or solely whitespace, and strips it."""
        stripped = v.strip()
        if not stripped:
            raise ValueError("Notification ID cannot be empty or consist only of whitespace.")
        return stripped
