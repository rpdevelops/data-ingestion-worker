"""
Repository for job operations.
"""
from sqlalchemy.orm import Session
from sqlalchemy import and_
from datetime import datetime
from typing import Optional

from src.models.job import Job, JobStatus
from src.app.logging_config import get_logger

logger = get_logger(__name__)


class JobRepository:
    """Repository for job database operations."""
    
    @staticmethod
    def get_by_id(db: Session, job_id: int) -> Optional[Job]:
        """
        Get job by ID.
        
        Args:
            db: Database session
            job_id: Job ID
            
        Returns:
            Job instance or None
        """
        return db.query(Job).filter(Job.job_id == job_id).first()
    
    @staticmethod
    def update_status(
        db: Session,
        job_id: int,
        status: JobStatus,
        process_start: Optional[datetime] = None,
        process_end: Optional[datetime] = None
    ) -> Job:
        """
        Update job status.
        
        Args:
            db: Database session
            job_id: Job ID
            status: New status
            process_start: Process start timestamp
            process_end: Process end timestamp
            
        Returns:
            Updated job instance
        """
        job = JobRepository.get_by_id(db, job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        
        job.job_status = status
        if process_start:
            job.job_process_start = process_start
        if process_end:
            job.job_process_end = process_end
        
        db.commit()
        db.refresh(job)
        
        logger.info(
            "Job status updated",
            extra={"job_id": job_id, "status": status.value}
        )
        
        return job
    
    @staticmethod
    def update_metadata(
        db: Session,
        job_id: int,
        total_rows: Optional[int] = None,
        processed_rows: Optional[int] = None,
        issue_count: Optional[int] = None
    ) -> Job:
        """
        Update job metadata.
        
        Args:
            db: Database session
            job_id: Job ID
            total_rows: Total rows count
            processed_rows: Processed rows count
            issue_count: Issue count
            
        Returns:
            Updated job instance
        """
        job = JobRepository.get_by_id(db, job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        
        if total_rows is not None:
            job.job_total_rows = total_rows
        if processed_rows is not None:
            job.job_processed_rows = processed_rows
        if issue_count is not None:
            job.job_issue_count = issue_count
        
        db.commit()
        db.refresh(job)
        
        return job
