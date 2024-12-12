from sqlalchemy import ForeignKey, Integer, Column, String, Text, Sequence, MetaData
from sqlalchemy.orm import relationship

from com.gwngames.server.entity.base.BaseEntity import BaseEntity


class GoogleScholarCitation(BaseEntity):
    """
    Represents a citation specific to a Google Scholar publication.
    """
    __tablename__ = 'google_scholar_citation'
    negative_seq = Sequence('citation_negative_seq', start=-1, increment=-1, metadata=MetaData())

    CLASS_ID = 1020
    VARIANT_ID = 50

    id = Column(Integer, negative_seq, primary_key=True, autoincrement=True)
    publication_id = Column(Integer, ForeignKey('google_scholar_publication.id'), nullable=False)
    citation_link = Column(Text, unique=True, nullable=False)
    year = Column(Integer, nullable=False)
    citations = Column(Integer, nullable=False)

    title = Column(String, nullable=False)
    link = Column(Text, primary_key=True)  # Citation link to the publication
    summary = Column(Text, nullable=True)  # Summary of the cited document
    cites_id = Column(String, primary_key=True)  # Unique ID for the citation
    document_link = Column(Text, nullable=True)  # Link to the full document (PDF or other)
    author_ids = Column(Text, nullable=True)  # Comma-separated list of author IDs
    profile_urls = Column(Text, nullable=True)  # Comma-separated list of profile URLs

    # Reference to the GoogleScholarPublication
    google_scholar_publication = relationship("GoogleScholarPublication", backref="citations")

    def __repr__(self):
        return f"<GoogleScholarCitation(id={self.id}, title={self.title}, cites_id={self.cites_id})>"
