from sqlalchemy import Column, Integer, String, Text, Date, ForeignKey
from sqlalchemy.orm import relationship, declared_attr

from com.gwngames.server.entity.base.BaseEntity import BaseEntity


class Publication(BaseEntity):
    """
    Represents a publication entity.
    """
    __tablename__ = 'publication'

    CLASS_ID = 1010
    VARIANT_ID = 1

    id = Column(Integer, autoincrement=True, primary_key=True)
    title = Column(String, unique=True, nullable=False)
    url = Column(Text)

    publication_year = Column(Integer, nullable=True)
    pages = Column(String)
    publisher = Column(String)
    description = Column(Text)

    journal_id = Column(Integer, ForeignKey('journal.id'), nullable=True)
    journal = relationship("Journal", back_populates="publications")

    conference_id = Column(Integer, ForeignKey('conference.id'), nullable=True)
    conference = relationship("Conference", back_populates="publications")

    def __repr__(self):
        return f"<Publication(title={self.title})>"
