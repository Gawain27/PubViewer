from sqlalchemy import Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import relationship

from com.gwngames.server.entity.base.BaseEntity import BaseEntity


class GoogleScholarAuthor(BaseEntity):
    """
    Represents a Google Scholar-specific author entity, referencing the base Author entity.
    """
    __tablename__ = 'google_scholar_author'

    CLASS_ID = 1000
    VARIANT_ID = 50

    id = Column(Integer, primary_key=True, autoincrement=True)
    author_key = Column(Integer, ForeignKey('author.id', ondelete='CASCADE'), nullable=False)
    profile_url = Column(Text, nullable=False)  # Specific to Google Scholar profiles
    author_id = Column(String, nullable=False, unique=True)  # Google Scholar ID for the author
    verified = Column(String, nullable=True)
    h_index = Column(Integer, nullable=True)
    i10_index = Column(Integer, nullable=True)

    author = relationship("Author", backref="google_scholar_profile")

    def __repr__(self):
        return f"<GoogleScholarAuthor(author_id={self.author_id}, profile_url={self.profile_url}, h_index={self.h_index})>"
