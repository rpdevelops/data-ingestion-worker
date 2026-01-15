"""
Repository for contact operations.
"""
from sqlalchemy.orm import Session
from typing import List, Set

from src.models.contact import Contact
from src.models.staging import Staging
from src.app.logging_config import get_logger

logger = get_logger(__name__)


class ContactRepository:
    """Repository for contact database operations."""
    
    @staticmethod
    def get_existing_emails(db: Session, emails: List[str]) -> Set[str]:
        """
        Get set of emails that already exist in contacts table.
        
        Args:
            db: Database session
            emails: List of normalized emails to check
            
        Returns:
            Set of existing emails
        """
        if not emails:
            return set()
        
        existing = db.query(Contact.contact_email).filter(
            Contact.contact_email.in_(emails)
        ).all()
        
        return {email[0] for email in existing}
    
    @staticmethod
    def create_from_staging(
        db: Session,
        staging: Staging
    ) -> Contact:
        """
        Create contact from staging record.
        
        Args:
            db: Database session
            staging: Staging record
            
        Returns:
            Created contact instance
        """
        if not staging.staging_email or not staging.staging_first_name or \
           not staging.staging_last_name or not staging.staging_company:
            raise ValueError("Staging record missing required fields for contact creation")
        
        contact = Contact(
            staging_id=staging.staging_id,
            contact_email=staging.staging_email,
            contact_first_name=staging.staging_first_name,
            contact_last_name=staging.staging_last_name,
            contact_company=staging.staging_company
        )
        
        db.add(contact)
        db.commit()
        db.refresh(contact)
        
        logger.debug(
            "Contact created from staging",
            extra={
                "contact_id": contact.contact_id,
                "staging_id": staging.staging_id,
                "email": contact.contact_email
            }
        )
        
        return contact
    
    @staticmethod
    def batch_create_from_staging(
        db: Session,
        staging_records: List[Staging]
    ) -> List[Contact]:
        """
        Batch create contacts from staging records.
        
        Args:
            db: Database session
            staging_records: List of staging records
            
        Returns:
            List of created contacts
        """
        contacts = []
        
        for staging in staging_records:
            try:
                contact = ContactRepository.create_from_staging(db, staging)
                contacts.append(contact)
            except ValueError as e:
                logger.warning(
                    "Failed to create contact from staging",
                    extra={
                        "staging_id": staging.staging_id,
                        "error": str(e)
                    }
                )
        
        return contacts
