"""
Main processor for CSV files.
Handles both initial processing and reprocessing flows.
"""
from sqlalchemy.orm import Session
from sqlalchemy import and_
from datetime import datetime
from typing import Dict, Set, List
import csv
from io import StringIO

from src.models.job import Job, JobStatus
from src.models.staging import Staging, StagingStatus
from src.models.issue import IssueType
from src.repositories.job_repository import JobRepository
from src.repositories.staging_repository import StagingRepository
from src.repositories.issue_repository import IssueRepository
from src.repositories.contact_repository import ContactRepository
from src.validators.row_validator import RowValidator, ValidationResult
from src.services.s3_service import s3_service
from src.app.logging_config import get_logger

logger = get_logger(__name__)


class Processor:
    """Processor for CSV file ingestion."""
    
    def __init__(self, db: Session):
        """
        Initialize processor.
        
        Args:
            db: Database session
        """
        self.db = db
        self.job_repo = JobRepository
        self.staging_repo = StagingRepository
        self.issue_repo = IssueRepository
        self.contact_repo = ContactRepository
    
    def process_job(self, job_id: int, s3_key: str) -> None:
        """
        Process a job - determines if it's initial processing or reprocessing.
        
        Args:
            job_id: Job ID
            s3_key: S3 object key for CSV file
        """
        logger.info(
            "Starting job processing",
            extra={"job_id": job_id, "s3_key": s3_key}
        )
        
        # Get job
        job = self.job_repo.get_by_id(self.db, job_id)
        if not job:
            # Job not found - might have been deleted or message is stale
            # Log warning but don't fail - just skip processing
            logger.warning(
                "Job not found - skipping processing",
                extra={
                    "job_id": job_id,
                    "s3_key": s3_key,
                    "note": "Job may have been deleted or message is stale"
                }
            )
            return
        
        # Check if job is already COMPLETED - skip processing if so
        if job.job_status == JobStatus.COMPLETED:
            logger.info(
                "Job is already COMPLETED - skipping processing",
                extra={
                    "job_id": job_id,
                    "status": job.job_status.value
                }
            )
            return
        
        # Check if job has staging records (reprocessing flow)
        has_staging = self.staging_repo.has_staging_records(self.db, job_id)
        
        if has_staging:
            logger.info(
                "Job has existing staging records - routing to REPROCESSING flow",
                extra={"job_id": job_id, "flow_type": "REPROCESSING"}
            )
            self._process_reprocessing(job_id)
        else:
            logger.info(
                "Job has no staging records - routing to INITIAL PROCESSING flow",
                extra={"job_id": job_id, "flow_type": "INITIAL_PROCESSING"}
            )
            self._process_initial(job_id, s3_key)
    
    def _process_initial(self, job_id: int, s3_key: str) -> None:
        """
        Process job for the first time (initial processing).
        
        Args:
            job_id: Job ID
            s3_key: S3 object key
        """
        logger.info(
            "Processing job - INITIAL PROCESSING flow",
            extra={"job_id": job_id, "flow_type": "INITIAL_PROCESSING"}
        )
        
        try:
            # Update job status to PROCESSING
            self.job_repo.update_status(
                self.db,
                job_id,
                JobStatus.PROCESSING,
                process_start=datetime.utcnow()
            )
            
            # Read CSV file from S3
            rows = s3_service.read_csv_file(s3_key)
            
            if not rows:
                raise ValueError("CSV file is empty")
            
            # Pre-processing: Normalize emails and identify duplicates within CSV
            duplicate_emails = self._identify_duplicate_emails(rows)
            
            # Get user_id from job
            job = self.job_repo.get_by_id(self.db, job_id)
            if not job:
                raise ValueError(f"Job {job_id} not found")
            user_id = job.job_user_id
            
            # Pre-load existing emails from contacts table (filtered by user_id)
            all_emails = {RowValidator.normalize_email(row.get("email", "")) for row in rows if row.get("email")}
            existing_emails = self.contact_repo.get_existing_emails(self.db, list(all_emails), user_id)
            
            logger.info(
                "Pre-processing complete",
                extra={
                    "job_id": job_id,
                    "total_rows": len(rows),
                    "duplicate_emails_count": len(duplicate_emails),
                    "existing_emails_count": len(existing_emails)
                }
            )
            
            # Process each row
            processed_count = 0
            issue_count = 0
            
            for row_number, row_data in enumerate(rows, start=1):
                try:
                    # Log row data being processed (only first few rows for debugging)
                    if row_number <= 3:
                        logger.debug(
                            "Processing row",
                            extra={
                                "job_id": job_id,
                                "row_number": row_number,
                                "row_data": row_data,
                                "row_data_keys": list(row_data.keys()) if row_data else [],
                                "row_data_values": list(row_data.values()) if row_data else [],
                                "email": row_data.get("email") if row_data else None,
                                "first_name": row_data.get("first_name") if row_data else None,
                                "last_name": row_data.get("last_name") if row_data else None,
                                "company": row_data.get("company") if row_data else None
                            }
                        )
                    
                    # Generate row hash
                    row_hash = self.staging_repo.generate_row_hash(
                        job_id,
                        row_number,
                        row_data
                    )
                    
                    # Check if row already processed (idempotency)
                    if self.staging_repo.exists_by_hash(self.db, job_id, row_hash):
                        logger.debug(
                            "Row already processed, skipping",
                            extra={"job_id": job_id, "row_number": row_number, "row_hash": row_hash}
                        )
                        continue
                    
                    # Create staging record
                    staging = self.staging_repo.create(
                        self.db,
                        job_id,
                        email=row_data.get("email"),
                        first_name=row_data.get("first_name"),
                        last_name=row_data.get("last_name"),
                        company=row_data.get("company"),
                        row_hash=row_hash,
                        status=StagingStatus.ISSUE  # Default to ISSUE, will update if valid
                    )
                    
                    # Validate row
                    validation_result = RowValidator.validate_row(
                        row_data,
                        duplicate_emails,
                        existing_emails
                    )
                    
                    if validation_result.is_valid:
                        # Update staging status to READY
                        self.staging_repo.update_status(self.db, staging.staging_id, StagingStatus.READY)
                    else:
                        # Create or get issue
                        normalized_email = RowValidator.normalize_email(row_data.get("email", ""))
                        issue_key = normalized_email if normalized_email else f"row_{row_number}"
                        
                        issue = self.issue_repo.get_or_create(
                            self.db,
                            job_id,
                            validation_result.issue_type,
                            issue_key,
                            validation_result.message
                        )
                        
                        # Link staging to issue
                        self.issue_repo.link_staging_to_issue(self.db, issue.issue_id, staging.staging_id)
                        
                        # Keep staging status as ISSUE
                        issue_count += 1
                    
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(
                        "Error processing row",
                        extra={
                            "job_id": job_id,
                            "row_number": row_number,
                            "error": str(e)
                        },
                        exc_info=True
                    )
                    # Continue processing other rows
                    continue
            
            # Update job metadata
            self.job_repo.update_metadata(
                self.db,
                job_id,
                total_rows=len(rows),
                processed_rows=processed_count,
                issue_count=issue_count
            )
            
            # Post-processing decision
            if issue_count > 0:
                # Has issues - set status to NEEDS_REVIEW
                self.job_repo.update_status(
                    self.db,
                    job_id,
                    JobStatus.NEEDS_REVIEW,
                    process_end=datetime.utcnow()
                )
                logger.info(
                    "Job processing complete - needs review",
                    extra={
                        "job_id": job_id,
                        "total_rows": len(rows),
                        "processed_rows": processed_count,
                        "issue_count": issue_count,
                        "flow_type": "INITIAL_PROCESSING"
                    }
                )
            else:
                # No issues - proceed to consolidation
                logger.info(
                    "Job processing complete - no issues, proceeding to consolidation",
                    extra={
                        "job_id": job_id,
                        "total_rows": len(rows),
                        "processed_rows": processed_count,
                        "flow_type": "INITIAL_PROCESSING"
                    }
                )
                self._consolidate(job_id)
                
        except Exception as e:
            logger.error(
                "Error in initial processing",
                extra={"job_id": job_id, "error": str(e)},
                exc_info=True
            )
            self.job_repo.update_status(self.db, job_id, JobStatus.FAILED)
            raise
    
    def _process_reprocessing(self, job_id: int) -> None:
        """
        Reprocess job - validates staging records without reading CSV again.
        
        Args:
            job_id: Job ID
        """
        logger.info(
            "Processing job - REPROCESSING flow",
            extra={"job_id": job_id, "flow_type": "REPROCESSING"}
        )
        
        try:
            # Update job status to PROCESSING
            self.job_repo.update_status(
                self.db,
                job_id,
                JobStatus.PROCESSING,
                process_start=datetime.utcnow()
            )
            
            # Get all staging records for this job
            staging_records = self.staging_repo.get_by_job_id(self.db, job_id)
            
            if not staging_records:
                raise ValueError(f"No staging records found for job {job_id}")
            
            logger.info(
                "Found staging records for reprocessing",
                extra={
                    "job_id": job_id,
                    "staging_count": len(staging_records),
                    "flow_type": "REPROCESSING"
                }
            )
            
            # Pre-load existing emails from contacts table
            # Only consider non-DISCARD records for duplicate detection
            discard_status = StagingStatus.DISCARD
            non_discard_records = [
                staging for staging in staging_records
                if staging.staging_status != discard_status
            ]
            
            # Get user_id from job
            job = self.job_repo.get_by_id(self.db, job_id)
            if not job:
                raise ValueError(f"Job {job_id} not found")
            user_id = job.job_user_id
            
            all_emails = {
                RowValidator.normalize_email(staging.staging_email)
                for staging in non_discard_records
                if staging.staging_email
            }
            existing_emails = self.contact_repo.get_existing_emails(self.db, list(all_emails), user_id)
            
            # Identify duplicate emails within staging records (excluding DISCARD)
            email_to_staging = {}
            for staging in non_discard_records:
                if staging.staging_email:
                    normalized = RowValidator.normalize_email(staging.staging_email)
                    if normalized not in email_to_staging:
                        email_to_staging[normalized] = []
                    email_to_staging[normalized].append(staging)
            
            duplicate_emails = {
                email for email, stagings in email_to_staging.items()
                if len(stagings) > 1
            }
            
            # Process each staging record
            processed_count = 0
            issue_count = 0
            ready_count = 0
            discard_count = 0
            
            for staging in staging_records:
                # Skip records already marked as DISCARD (user decision)
                if staging.staging_status == StagingStatus.DISCARD:
                    logger.debug(
                        "Skipping DISCARD staging record",
                        extra={
                            "job_id": job_id,
                            "staging_id": staging.staging_id,
                            "flow_type": "REPROCESSING"
                        }
                    )
                    discard_count += 1
                    continue
                
                try:
                    # Build row data from staging record
                    row_data = {
                        "email": staging.staging_email or "",
                        "first_name": staging.staging_first_name or "",
                        "last_name": staging.staging_last_name or "",
                        "company": staging.staging_company or ""
                    }
                    
                    # Validate row using staging data
                    validation_result = RowValidator.validate_row(
                        row_data,
                        duplicate_emails,
                        existing_emails
                    )
                    
                    if validation_result.is_valid:
                        # Update staging status to READY
                        # Don't remove issue_items - just update status
                        self.staging_repo.update_status(self.db, staging.staging_id, StagingStatus.READY)
                        
                        # Check and mark related issues as resolved if all staging records are resolved
                        related_issues = self.issue_repo.get_issues_for_staging(self.db, staging.staging_id)
                        for issue in related_issues:
                            if not issue.issue_resolved:
                                self.issue_repo.check_and_mark_resolved_if_all_staging_resolved(
                                    self.db,
                                    issue.issue_id
                                )
                        
                        self.db.commit()
                        ready_count += 1
                        
                    else:
                        # Validation failed - create or get issue
                        normalized_email = RowValidator.normalize_email(row_data.get("email", ""))
                        issue_key = normalized_email if normalized_email else f"staging_{staging.staging_id}"
                        
                        issue = self.issue_repo.get_or_create(
                            self.db,
                            job_id,
                            validation_result.issue_type,
                            issue_key,
                            validation_result.message
                        )
                        
                        # If issue was previously resolved, check if it should be marked as unresolved
                        # (only if this staging record causes the issue to have unresolved staging records)
                        if issue.issue_resolved:
                            # Re-check if all staging records are still resolved
                            # This will automatically mark as unresolved if needed
                            staging_ids = [
                                item.item_staging_id 
                                for item in issue.issue_items
                            ]
                            
                            issue_status = StagingStatus.ISSUE
                            unresolved_count = self.db.query(Staging).filter(
                                and_(
                                    Staging.staging_id.in_(staging_ids),
                                    Staging.staging_status == issue_status
                                )
                            ).count()
                            
                            if unresolved_count > 0:
                                issue.issue_resolved = False
                                issue.issue_resolved_at = None
                                issue.issue_resolved_by = None
                                issue.issue_resolution_comment = None
                                logger.info(
                                    "Issue marked as unresolved due to validation failure",
                                    extra={
                                        "issue_id": issue.issue_id,
                                        "staging_id": staging.staging_id,
                                        "unresolved_staging_count": unresolved_count
                                    }
                                )
                        
                        # Link staging to issue (will check if already linked)
                        self.issue_repo.link_staging_to_issue(self.db, issue.issue_id, staging.staging_id)
                        
                        # Update staging status to ISSUE
                        self.staging_repo.update_status(self.db, staging.staging_id, StagingStatus.ISSUE)
                        self.db.commit()
                        issue_count += 1
                    
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(
                        "Error reprocessing staging record",
                        extra={
                            "job_id": job_id,
                            "staging_id": staging.staging_id,
                            "error": str(e)
                        },
                        exc_info=True
                    )
                    # Continue processing other records
                    continue
            
            # Update job metadata
            self.job_repo.update_metadata(
                self.db,
                job_id,
                processed_rows=processed_count,
                issue_count=issue_count
            )
            
            # Post-processing decision
            if issue_count > 0:
                # Has issues - set status to NEEDS_REVIEW
                self.job_repo.update_status(
                    self.db,
                    job_id,
                    JobStatus.NEEDS_REVIEW,
                    process_end=datetime.utcnow()
                )
                logger.info(
                    "Job reprocessing complete - needs review",
                    extra={
                        "job_id": job_id,
                        "processed_rows": processed_count,
                        "ready_count": ready_count,
                        "issue_count": issue_count,
                        "discard_count": discard_count,
                        "flow_type": "REPROCESSING"
                    }
                )
            else:
                # No issues - proceed to consolidation
                logger.info(
                    "Job reprocessing complete - no issues, proceeding to consolidation",
                    extra={
                        "job_id": job_id,
                        "processed_rows": processed_count,
                        "ready_count": ready_count,
                        "discard_count": discard_count,
                        "flow_type": "REPROCESSING"
                    }
                )
                self._consolidate(job_id)
                
        except Exception as e:
            logger.error(
                "Error in reprocessing",
                extra={"job_id": job_id, "error": str(e)},
                exc_info=True
            )
            self.job_repo.update_status(self.db, job_id, JobStatus.FAILED)
            raise
    
    def _identify_duplicate_emails(self, rows: List[Dict]) -> Set[str]:
        """
        Identify emails that appear multiple times in CSV.
        Any email appearing more than once is considered a duplicate.
        
        Args:
            rows: List of row dictionaries
            
        Returns:
            Set of normalized emails that are duplicates
        """
        email_to_rows = {}
        
        for row in rows:
            email = row.get("email", "").strip()
            if not email:
                continue
            
            normalized = RowValidator.normalize_email(email)
            
            if normalized not in email_to_rows:
                email_to_rows[normalized] = []
            
            email_to_rows[normalized].append(row)
        
        # Find emails that appear multiple times (any occurrence > 1 is a duplicate)
        duplicate_emails = set()
        
        for email, email_rows in email_to_rows.items():
            if len(email_rows) > 1:
                # Any email appearing more than once is a duplicate
                # This includes both cases:
                # - Same email with different identities (conflict to resolve)
                # - Same email with same identity (error/redundancy to handle)
                duplicate_emails.add(email)
                
                logger.debug(
                    "Duplicate email detected",
                    extra={
                        "email": email,
                        "occurrence_count": len(email_rows),
                        "rows": [
                            {
                                "first_name": row.get("first_name"),
                                "last_name": row.get("last_name"),
                                "company": row.get("company")
                            }
                            for row in email_rows
                        ]
                    }
                )
        
        return duplicate_emails
    
    def _consolidate(self, job_id: int) -> None:
        """
        Consolidate staging records to contacts table.
        
        Args:
            job_id: Job ID
        """
        logger.info(
            "Starting consolidation",
            extra={"job_id": job_id}
        )
        
        try:
            # Get job to obtain user_id
            job = self.job_repo.get_by_id(self.db, job_id)
            if not job:
                raise ValueError(f"Job {job_id} not found")
            user_id = job.job_user_id
            
            # Get staging records ready for consolidation
            ready_staging = self.staging_repo.get_ready_for_consolidation(self.db, job_id)
            
            if not ready_staging:
                logger.warning(
                    "No staging records ready for consolidation",
                    extra={"job_id": job_id}
                )
                self.job_repo.update_status(self.db, job_id, JobStatus.COMPLETED)
                return
            
            # Batch create contacts from staging records (with user_id)
            contacts = self.contact_repo.batch_create_from_staging(self.db, ready_staging, user_id)
            
            # Update staging records to SUCCESS
            for staging in ready_staging:
                self.staging_repo.update_status(self.db, staging.staging_id, StagingStatus.SUCCESS)
            
            # Update job status to COMPLETED
            self.job_repo.update_status(
                self.db,
                job_id,
                JobStatus.COMPLETED,
                process_end=datetime.utcnow()
            )
            
            logger.info(
                "Consolidation complete",
                extra={
                    "job_id": job_id,
                    "contacts_created": len(contacts)
                }
            )
            
        except Exception as e:
            logger.error(
                "Error in consolidation",
                extra={"job_id": job_id, "error": str(e)},
                exc_info=True
            )
            self.job_repo.update_status(self.db, job_id, JobStatus.FAILED)
            raise
