"""
SQLAlchemy model for jobs table.
"""
from sqlalchemy import Column, Integer, String, DateTime, Enum as SQLEnum
from sqlalchemy.sql import func
import enum

from src.app.db.database import Base


class JobStatus(str, enum.Enum):
    """Job status enumeration."""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Job(Base):
    """Job model representing the jobs table."""
    
    __tablename__ = "jobs"
    
    job_id = Column(Integer, primary_key=True, index=True)
    job_created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    job_user_id = Column(String, nullable=False, index=True)
    job_original_filename = Column(String, nullable=False)
    job_s3_object_key = Column(String, nullable=False)
    job_status = Column(SQLEnum(JobStatus), nullable=False, index=True)
    job_total_rows = Column(Integer, nullable=False, default=0)
    job_processed_rows = Column(Integer, nullable=False, default=0)
    job_issue_count = Column(Integer, nullable=False, default=0)
    job_process_start = Column(DateTime(timezone=True), nullable=True)
    job_process_end = Column(DateTime(timezone=True), nullable=True)
    
    def __repr__(self):
        return f"<Job(job_id={self.job_id}, status={self.job_status}, user_id={self.job_user_id})>"
