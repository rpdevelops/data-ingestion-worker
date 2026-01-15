"""
Repository for staging operations.
"""
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from typing import List, Optional, Dict
import hashlib
import json

from src.models.staging import Staging, StagingStatus
from src.app.logging_config import get_logger

logger = get_logger(__name__)


class StagingRepository:
    """Repository for staging database operations."""
    
    @staticmethod
    def generate_row_hash(job_id: int, row_number: int, row_data: Dict) -> str:
        """
        Generate deterministic hash for a row.
        
        Args:
            job_id: Job ID
            row_number: Row number in CSV
            row_data: Row data dictionary
            
        Returns:
            Hash string
        """
        # Normalize row data for consistent hashing
        normalized = {
            "job_id": job_id,
            "row_number": row_number,
            "email": str(row_data.get("email", "")).lower().strip(),
            "first_name": str(row_data.get("first_name", "")).strip(),
            "last_name": str(row_data.get("last_name", "")).strip(),
            "company": str(row_data.get("company", "")).strip(),
        }
        
        # Create hash from normalized data
        hash_input = json.dumps(normalized, sort_keys=True)
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    @staticmethod
    def exists_by_hash(db: Session, job_id: int, row_hash: str) -> bool:
        """
        Check if staging record exists by hash.
        
        Args:
            db: Database session
            job_id: Job ID
            row_hash: Row hash
            
        Returns:
            True if exists, False otherwise
        """
        count = db.query(Staging).filter(
            and_(
                Staging.staging_job_id == job_id,
                Staging.staging_row_hash == row_hash
            )
        ).count()
        
        return count > 0
    
    @staticmethod
    def create(
        db: Session,
        job_id: int,
        email: Optional[str],
        first_name: Optional[str],
        last_name: Optional[str],
        company: Optional[str],
        row_hash: str,
        status: StagingStatus = StagingStatus.ISSUE
    ) -> Staging:
        """
        Create staging record.
        
        Args:
            db: Database session
            job_id: Job ID
            email: Email address
            first_name: First name
            last_name: Last name
            company: Company name
            row_hash: Row hash
            status: Initial status
            
        Returns:
            Created staging instance
        """
        staging = Staging(
            staging_job_id=job_id,
            staging_email=email,
            staging_first_name=first_name,
            staging_last_name=last_name,
            staging_company=company,
            staging_row_hash=row_hash,
            staging_status=status
        )
        
        db.add(staging)
        db.commit()
        db.refresh(staging)
        
        return staging
    
    @staticmethod
    def get_by_job_id(db: Session, job_id: int) -> List[Staging]:
        """
        Get all staging records for a job.
        
        Args:
            db: Database session
            job_id: Job ID
            
        Returns:
            List of staging records
        """
        return db.query(Staging).filter(
            Staging.staging_job_id == job_id
        ).all()
    
    @staticmethod
    def get_ready_for_consolidation(db: Session, job_id: int) -> List[Staging]:
        """
        Get staging records ready for consolidation.
        
        Args:
            db: Database session
            job_id: Job ID
            
        Returns:
            List of staging records with READY status
        """
        return db.query(Staging).filter(
            and_(
                Staging.staging_job_id == job_id,
                Staging.staging_status == StagingStatus.READY
            )
        ).all()
    
    @staticmethod
    def update_status(
        db: Session,
        staging_id: int,
        status: StagingStatus
    ) -> Staging:
        """
        Update staging record status.
        
        Args:
            db: Database session
            staging_id: Staging ID
            status: New status
            
        Returns:
            Updated staging instance
        """
        staging = db.query(Staging).filter(Staging.staging_id == staging_id).first()
        if not staging:
            raise ValueError(f"Staging {staging_id} not found")
        
        staging.staging_status = status
        db.commit()
        db.refresh(staging)
        
        return staging
    
    @staticmethod
    def count_by_status(db: Session, job_id: int, status: StagingStatus) -> int:
        """
        Count staging records by status.
        
        Args:
            db: Database session
            job_id: Job ID
            status: Status to count
            
        Returns:
            Count of records
        """
        return db.query(Staging).filter(
            and_(
                Staging.staging_job_id == job_id,
                Staging.staging_status == status
            )
        ).count()
    
    @staticmethod
    def has_staging_records(db: Session, job_id: int) -> bool:
        """
        Check if job has any staging records.
        
        Args:
            db: Database session
            job_id: Job ID
            
        Returns:
            True if has staging records, False otherwise
        """
        count = db.query(Staging).filter(
            Staging.staging_job_id == job_id
        ).count()
        
        return count > 0
