from sqlalchemy import Date, Column, Integer, ForeignKey, String, Text
from sqlalchemy.orm import relationship

from com.gwngames.server.entity.base.BaseEntity import BaseEntity


class Conference(BaseEntity):
    """
    Represents a conference entity.
    """
    __tablename__ = 'conference'

    CLASS_ID = 1040
    VARIANT_ID = 150

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    acronym = Column(String, nullable=True)
    publisher = Column(String, nullable=True)
    year = Column(Integer, nullable=True)
    rank = Column(String, nullable=True)
    note = Column(Text, nullable=True)
    dblp_link = Column(Text, nullable=True)
    primary_for = Column(String, nullable=True)
    comments = Column(Integer, nullable=True)
    average_rating = Column(String, nullable=True)

    publications = relationship("Publication", back_populates="conference")


    def __repr__(self):
        return f"<Conference(title={self.title}, acronym={self.acronym}, rank={self.rank}, year={self.year})>"
