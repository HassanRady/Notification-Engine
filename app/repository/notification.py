from sqlalchemy import Column, String, Boolean
from app.repository.database import Base

class UserDndSetting(Base):
    """
    SQLAlchemy model representing a user's Do-Not-Disturb (DND) configuration status.
    Used by the Cache-Aside DB fallback resolver.
    """
    __tablename__ = "user_dnd_settings"

    user_id = Column(String, primary_key=True, index=True, nullable=False)
    dnd_active = Column(Boolean, default=False, nullable=False)
