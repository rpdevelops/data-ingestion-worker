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
    def get_existing_emails(db: Session, emails: List[str], user_id: str) -> Set[str]:
        """
        Get set of emails that already exist in contacts table for a specific user.
        Only checks emails that belong to the same user_id.
        
        Args:
            db: Database session
            emails: List of normalized emails to check
            user_id: User ID to filter by
            
        Returns:
            Set of existing emails for the user
        """
        if not emails:
            return set()
        
        existing = db.query(Contact.contact_email).filter(
            Contact.contact_email.in_(emails),
            Contact.contacts_user_id == user_id
        ).all()
        
        return {email[0] for email in existing}
    
    @staticmethod
    def create_from_staging(
        db: Session,
        staging: Staging,
        user_id: str
    ) -> Contact:
        """
        Create contact from staging record.
        
        Args:
            db: Database session
            staging: Staging record
            user_id: User ID to associate with the contact
            
        Returns:
            Created contact instance
        """
        if not staging.staging_email or not staging.staging_first_name or \
           not staging.staging_last_name or not staging.staging_company:
            raise ValueError("Staging record missing required fields for contact creation")
        
        if not user_id:
            raise ValueError("user_id is required for contact creation")
        
        contact = Contact(
            staging_id=staging.staging_id,
            contacts_user_id=user_id,
            contact_email=staging.staging_email,
            contact_first_name=staging.staging_first_name,
            contact_last_name=staging.staging_last_name,
            contact_company=staging.staging_company
        )
        
        logger.debug(
            "Creating contact with user_id",
            extra={
                "staging_id": staging.staging_id,
                "user_id": user_id,
                "email": staging.staging_email
            }
        )
        
        db.add(contact)
        db.commit()
        db.refresh(contact)
        
        logger.debug(
            "Contact created from staging",
            extra={
                "contact_id": contact.contact_id,
                "staging_id": staging.staging_id,
                "email": contact.contact_email,
                "user_id": user_id
            }
        )
        
        return contact
    
    @staticmethod
    def batch_create_from_staging(
        db: Session,
        staging_records: List[Staging],
        user_id: str
    ) -> List[Contact]:
        """
        Batch create contacts from staging records.
        
        Args:
            db: Database session
            staging_records: List of staging records
            user_id: User ID to associate with all contacts
            
        Returns:
            List of created contacts
        """
        contacts = []
        
        for staging in staging_records:
            try:
                contact = ContactRepository.create_from_staging(db, staging, user_id)
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
