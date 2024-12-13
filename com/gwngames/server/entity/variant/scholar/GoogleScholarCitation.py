import threading

from sqlalchemy import ForeignKey, Integer, Column, String, Text
from sqlalchemy.orm import relationship

from com.gwngames.config.Context import Context
from com.gwngames.server.entity.base.BaseEntity import BaseEntity


class GoogleScholarCitation(BaseEntity):
    """
    Represents a citation specific to a Google Scholar publication.
    """
    __tablename__ = 'google_scholar_citation'
    _seq_lock = threading.Lock()

    CLASS_ID = 1020
    VARIANT_ID = 50

    def __init__(self, *args, **kwargs):
        if 'id' not in kwargs:
            kwargs['id'] = self._get_next_value()
        super().__init__(**kwargs)

    @classmethod
    def _get_next_value(cls):
        with cls._seq_lock:
            next_value = Context().get_config().get_value("pub_citation_neg_counter")
            Context().get_config().set_and_save("pub_citation_neg_counter", next_value - 1)
            return next_value

    id = Column(Integer, primary_key=True)
    publication_id = Column(Integer, ForeignKey('google_scholar_publication.id'), nullable=False)
    citation_link = Column(Text, primary_key=True)
    year = Column(Integer, nullable=False)
    citations = Column(Integer, nullable=False)

    title = Column(String, nullable=True)
    link = Column(Text, nullable=True)  # Citation link to the publication
    summary = Column(Text, nullable=True)  # Summary of the cited document
    cites_id = Column(String, primary_key=True)  # Unique ID for the citation
    document_link = Column(Text, nullable=True)  # Link to the full document (PDF or other)
    author_ids = Column(Text, nullable=True)  # Comma-separated list of author IDs
    profile_urls = Column(Text, nullable=True)  # Comma-separated list of profile URLs

    # Reference to the GoogleScholarPublication
    google_scholar_publication = relationship("GoogleScholarPublication", backref="citations")

    def __repr__(self):
        return f"<GoogleScholarCitation(id={self.id}, title={self.title}, cites_id={self.cites_id})>"
