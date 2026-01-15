"""
SQLAlchemy model for staging table.
"""
from sqlalchemy import Column, BigInteger, Integer, String, DateTime, Enum as SQLEnum, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import enum

from src.app.db.database import Base


class StagingStatus(str, enum.Enum):
    """Staging status enumeration."""
    READY = "READY"
    SUCCESS = "SUCCESS"
    DISCARD = "DISCARD"
    ISSUE = "ISSUE"


class Staging(Base):
    """Staging model representing the staging table."""
    
    __tablename__ = "staging"
    
    staging_id = Column(BigInteger, primary_key=True, index=True)
    staging_job_id = Column(Integer, ForeignKey("jobs.job_id", ondelete="CASCADE"), nullable=False, index=True)
    staging_email = Column(String, nullable=True)
    staging_first_name = Column(String, nullable=True)
    staging_last_name = Column(String, nullable=True)
    staging_company = Column(String, nullable=True)
    staging_created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    staging_status = Column(SQLEnum(StagingStatus), nullable=True, index=True)
    staging_row_hash = Column(String, nullable=False)
    
    # Relationships
    issue_items = relationship("IssueItem", back_populates="staging", cascade="all, delete-orphan")
    contact = relationship("Contact", back_populates="staging", uselist=False, cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Staging(staging_id={self.staging_id}, job_id={self.staging_job_id}, status={self.staging_status})>"
