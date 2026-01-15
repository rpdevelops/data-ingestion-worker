"""
Repository for issue operations.
"""
from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import Optional

from src.models.issue import Issue, IssueType
from src.models.issue_item import IssueItem
from src.app.logging_config import get_logger

logger = get_logger(__name__)


class IssueRepository:
    """Repository for issue database operations."""
    
    @staticmethod
    def get_or_create(
        db: Session,
        job_id: int,
        issue_type: IssueType,
        issue_key: str,
        issue_description: Optional[str] = None
    ) -> Issue:
        """
        Get or create issue idempotently.
        
        Args:
            db: Database session
            job_id: Job ID
            issue_type: Issue type
            issue_key: Issue key (e.g., normalized email)
            issue_description: Issue description
            
        Returns:
            Issue instance
        """
        # Try to get existing issue
        issue = db.query(Issue).filter(
            and_(
                Issue.issues_job_id == job_id,
                Issue.issue_type == issue_type,
                Issue.issue_key == issue_key
            )
        ).first()
        
        if issue:
            return issue
        
        # Create new issue
        issue = Issue(
            issues_job_id=job_id,
            issue_type=issue_type,
            issue_key=issue_key,
            issue_description=issue_description,
            issue_resolved=False
        )
        
        db.add(issue)
        db.commit()
        db.refresh(issue)
        
        logger.debug(
            "Issue created",
            extra={
                "issue_id": issue.issue_id,
                "job_id": job_id,
                "issue_type": issue_type.value,
                "issue_key": issue_key
            }
        )
        
        return issue
    
    @staticmethod
    def link_staging_to_issue(
        db: Session,
        issue_id: int,
        staging_id: int
    ) -> IssueItem:
        """
        Link staging record to issue.
        
        Args:
            db: Database session
            issue_id: Issue ID
            staging_id: Staging ID
            
        Returns:
            IssueItem instance
        """
        # Check if link already exists
        existing = db.query(IssueItem).filter(
            and_(
                IssueItem.item_issue_id == issue_id,
                IssueItem.item_staging_id == staging_id
            )
        ).first()
        
        if existing:
            return existing
        
        # Create new link
        issue_item = IssueItem(
            item_issue_id=issue_id,
            item_staging_id=staging_id
        )
        
        db.add(issue_item)
        db.commit()
        db.refresh(issue_item)
        
        return issue_item
    
    @staticmethod
    def get_by_job_id(db: Session, job_id: int) -> list[Issue]:
        """
        Get all issues for a job.
        
        Args:
            db: Database session
            job_id: Job ID
            
        Returns:
            List of issues
        """
        return db.query(Issue).filter(Issue.issues_job_id == job_id).all()
    
    @staticmethod
    def mark_as_resolved(
        db: Session,
        issue_id: int,
        resolved_by: Optional[str] = None,
        resolution_comment: Optional[str] = None
    ) -> Issue:
        """
        Mark issue as resolved.
        
        Args:
            db: Database session
            issue_id: Issue ID
            resolved_by: User who resolved the issue
            resolution_comment: Resolution comment
            
        Returns:
            Updated issue instance
        """
        from datetime import datetime
        from src.models.issue import Issue
        
        issue = db.query(Issue).filter(Issue.issue_id == issue_id).first()
        if not issue:
            raise ValueError(f"Issue {issue_id} not found")
        
        issue.issue_resolved = True
        issue.issue_resolved_at = datetime.utcnow()
        if resolved_by:
            issue.issue_resolved_by = resolved_by
        if resolution_comment:
            issue.issue_resolution_comment = resolution_comment
        
        db.commit()
        db.refresh(issue)
        
        logger.info(
            "Issue marked as resolved",
            extra={
                "issue_id": issue_id,
                "resolved_by": resolved_by
            }
        )
        
        return issue
    
    @staticmethod
    def get_issues_for_staging(db: Session, staging_id: int) -> list[Issue]:
        """
        Get all issues linked to a staging record.
        
        Args:
            db: Database session
            staging_id: Staging ID
            
        Returns:
            List of issues
        """
        return db.query(Issue).join(IssueItem).filter(
            IssueItem.item_staging_id == staging_id
        ).all()
    
    @staticmethod
    def check_and_mark_resolved_if_all_staging_resolved(
        db: Session,
        issue_id: int
    ) -> bool:
        """
        Check if all staging records for an issue are resolved (READY, SUCCESS, or DISCARD).
        If so, mark the issue as resolved.
        
        Args:
            db: Database session
            issue_id: Issue ID
            
        Returns:
            True if issue was marked as resolved, False otherwise
        """
        from src.models.staging import Staging, StagingStatus
        from src.models.issue_item import IssueItem
        
        # Get all staging records linked to this issue
        staging_ids = db.query(IssueItem.item_staging_id).filter(
            IssueItem.item_issue_id == issue_id
        ).all()
        
        if not staging_ids:
            return False
        
        staging_ids = [sid[0] for sid in staging_ids]
        
        # Check if all staging records are resolved (READY, SUCCESS, or DISCARD)
        unresolved_count = db.query(Staging).filter(
            and_(
                Staging.staging_id.in_(staging_ids),
                Staging.staging_status == StagingStatus.ISSUE
            )
        ).count()
        
        # If no unresolved staging records, mark issue as resolved
        if unresolved_count == 0:
            IssueRepository.mark_as_resolved(
                db,
                issue_id,
                resolved_by="system",
                resolution_comment="All related staging records resolved during reprocessing"
            )
            return True
        
        return False
