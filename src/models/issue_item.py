"""
SQLAlchemy model for issue_items table.
"""
from sqlalchemy import Column, Integer, BigInteger, ForeignKey
from sqlalchemy.orm import relationship

from src.app.db.database import Base


class IssueItem(Base):
    """IssueItem model representing the issue_items table."""
    
    __tablename__ = "issue_items"
    
    issue_item_id = Column(Integer, primary_key=True, index=True)
    item_issue_id = Column(Integer, ForeignKey("issues.issue_id", ondelete="CASCADE"), nullable=False, index=True)
    item_staging_id = Column(BigInteger, ForeignKey("staging.staging_id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Relationships
    issue = relationship("Issue", back_populates="issue_items")
    staging = relationship("Staging", back_populates="issue_items")
    
    def __repr__(self):
        return f"<IssueItem(issue_item_id={self.issue_item_id}, issue_id={self.item_issue_id}, staging_id={self.item_staging_id})>"
