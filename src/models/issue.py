"""
SQLAlchemy model for issues table.
"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Enum as SQLEnum, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import enum

from src.app.db.database import Base


class IssueType(str, enum.Enum):
    """Issue type enumeration."""
    DUPLICATE_EMAIL = "DUPLICATE_EMAIL"
    INVALID_EMAIL = "INVALID_EMAIL"
    EXISTING_EMAIL = "EXISTING_EMAIL"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"


class Issue(Base):
    """Issue model representing the issues table."""
    
    __tablename__ = "issues"
    
    issue_id = Column(Integer, primary_key=True, index=True)
    issues_job_id = Column(Integer, ForeignKey("jobs.job_id", ondelete="CASCADE"), nullable=False, index=True)
    issue_type = Column(SQLEnum(IssueType), nullable=False)
    issue_key = Column(String, nullable=False)
    issue_resolved = Column(Boolean, nullable=False, default=False)
    issue_description = Column(String, nullable=True)
    issue_resolved_at = Column(DateTime(timezone=True), nullable=True)
    issue_resolved_by = Column(String, nullable=True)
    issue_resolution_comment = Column(String, nullable=True)
    issue_created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Relationships
    issue_items = relationship("IssueItem", back_populates="issue", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Issue(issue_id={self.issue_id}, job_id={self.issues_job_id}, type={self.issue_type}, resolved={self.issue_resolved})>"
