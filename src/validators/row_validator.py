"""
Row validation logic.
"""
import re
from typing import Dict, Set, Optional
from dataclasses import dataclass

from src.models.issue import IssueType
from src.app.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Validation result."""
    is_valid: bool
    issue_type: Optional[IssueType] = None
    message: Optional[str] = None


class RowValidator:
    """Validator for CSV row data."""
    
    # Email regex pattern (RFC 5322 compliant simplified)
    EMAIL_PATTERN = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )
    
    REQUIRED_FIELDS = ["email", "first_name", "last_name", "company"]
    
    @staticmethod
    def normalize_email(email: str) -> str:
        """
        Normalize email address.
        
        Args:
            email: Email address
            
        Returns:
            Normalized email (lowercase, trimmed)
        """
        if not email:
            return ""
        return email.lower().strip()
    
    @staticmethod
    def validate_row(
        row_data: Dict[str, str],
        duplicate_emails: Set[str],
        existing_emails: Set[str]
    ) -> ValidationResult:
        """
        Validate a single row.
        
        This method does NOT access the database directly.
        It uses pre-loaded data from memory for performance.
        
        Args:
            row_data: Row data dictionary with keys: email, first_name, last_name, company
            duplicate_emails: Set of normalized emails that appear multiple times in CSV
            existing_emails: Set of normalized emails that already exist in contacts table
            
        Returns:
            ValidationResult with validation status
        """
        # Step 1: Check required fields
        for field in RowValidator.REQUIRED_FIELDS:
            value = row_data.get(field, "").strip() if row_data.get(field) else ""
            if not value:
                return ValidationResult(
                    is_valid=False,
                    issue_type=IssueType.MISSING_REQUIRED_FIELD,
                    message=f"Missing required field: {field}"
                )
        
        email = row_data.get("email", "").strip()
        
        # Step 2: Validate email format
        if not RowValidator.EMAIL_PATTERN.match(email):
            return ValidationResult(
                is_valid=False,
                issue_type=IssueType.INVALID_EMAIL,
                message=f"Invalid email format: {email}"
            )
        
        # Normalize email for duplicate and existing checks
        normalized_email = RowValidator.normalize_email(email)
        
        # Step 3: Check for duplicate email within CSV
        if normalized_email in duplicate_emails:
            return ValidationResult(
                is_valid=False,
                issue_type=IssueType.DUPLICATE_EMAIL,
                message=f"Duplicate email in CSV: {email}"
            )
        
        # Step 4: Check if email already exists in contacts
        if normalized_email in existing_emails:
            return ValidationResult(
                is_valid=False,
                issue_type=IssueType.EXISTING_EMAIL,
                message=f"Email already exists in contacts: {email}"
            )
        
        # All validations passed
        return ValidationResult(is_valid=True)
