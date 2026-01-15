"""
SQLAlchemy model for contacts table.
"""
from sqlalchemy import Column, BigInteger, String, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from src.app.db.database import Base


class Contact(Base):
    """Contact model representing the contacts table."""
    
    __tablename__ = "contacts"
    
    contact_id = Column(BigInteger, primary_key=True, index=True)
    staging_id = Column(BigInteger, ForeignKey("staging.staging_id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    contact_email = Column(String, nullable=False, index=True)
    contact_first_name = Column(String, nullable=False)
    contact_last_name = Column(String, nullable=False)
    contact_company = Column(String, nullable=False)
    contact_created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Relationships
    staging = relationship("Staging", back_populates="contact")
    
    def __repr__(self):
        return f"<Contact(contact_id={self.contact_id}, email={self.contact_email}, staging_id={self.staging_id})>"
