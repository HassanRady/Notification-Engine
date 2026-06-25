from datetime import datetime, timezone
from typing import List, Dict, Any
import logging
from sqlalchemy import Column, String, Boolean, Integer, DateTime
from app.repository.database import Base, async_session_maker

logger = logging.getLogger(__name__)

class UserDndSetting(Base):
    """
    SQLAlchemy model representing a user's Do-Not-Disturb (DND) configuration status.
    Used by the Cache-Aside DB fallback resolver.
    """
    __tablename__ = "user_dnd_settings"

    user_id = Column(String, primary_key=True, index=True, nullable=False)
    dnd_active = Column(Boolean, default=False, nullable=False)

class NotificationState(Base):
    """
    SQLAlchemy model tracking the current delivery status of a notification.
    """
    __tablename__ = "notification_states"

    notification_id = Column(String, primary_key=True, index=True, nullable=False)
    status = Column(String, nullable=False)
    sequence_id = Column(Integer, nullable=False)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

async def bulk_upsert_notification_states(receipts: List[Dict[str, Any]]) -> None:
    """
    Performs high-performance bulk upserts of delivery receipt statuses.
    
    Integrates Optimistic Concurrency Control (OCC) using the sequence_id version counter
    to prevent out-of-order delivery state regressions due to distributed clock skew.
    
    Supports both PostgreSQL and SQLite dialect upserts to allow local testing.
    """
    if not receipts:
        return

    # Prepare values mapping
    values_list = []
    for r in receipts:
        ts = r.get("timestamp")
        # Ensure timezone-naive datetime objects for storage
        if isinstance(ts, str):
            try:
                # Remove Z and parse timezone-aware, then convert to naive UTC
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                ts = dt.astimezone(timezone.utc).replace(tzinfo=None)
            except ValueError:
                ts = datetime.utcnow()
        elif isinstance(ts, datetime):
            if ts.tzinfo is not None:
                ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            ts = datetime.utcnow()

        values_list.append({
            "notification_id": r["notification_id"],
            "status": str(r["status"]),
            "sequence_id": int(r["sequence_id"]),
            "updated_at": ts
        })

    async with async_session_maker() as session:
        dialect_name = session.bind.dialect.name
        
        if dialect_name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert
            stmt = pg_insert(NotificationState).values(values_list)
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=["notification_id"],
                set_={
                    "status": stmt.excluded.status,
                    "sequence_id": stmt.excluded.sequence_id,
                    "updated_at": stmt.excluded.updated_at
                },
                # OCC: update only if incoming sequence_id is strictly greater
                where=(stmt.excluded.sequence_id > NotificationState.sequence_id)
            )
        else:
            # Fallback for SQLite to facilitate unit testing
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert
            stmt = sqlite_insert(NotificationState).values(values_list)
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=["notification_id"],
                set_={
                    "status": stmt.excluded.status,
                    "sequence_id": stmt.excluded.sequence_id,
                    "updated_at": stmt.excluded.updated_at
                },
                # OCC: update only if incoming sequence_id is strictly greater
                where=(stmt.excluded.sequence_id > NotificationState.sequence_id)
            )

        await session.execute(upsert_stmt)
        await session.commit()
            
    logger.info(f"Successfully bulk upserted {len(values_list)} notification states.")
