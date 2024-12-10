from sqlalchemy import Column, Integer, ForeignKey, String, Text, Date
from sqlalchemy.orm import relationship

from com.gwngames.server.entity.base.BaseEntity import BaseEntity


class Journal(BaseEntity):
    """
    Represents a journal entity.
    """
    __tablename__ = 'journal'

    CLASS_ID = 1030
    VARIANT_ID = 200

    id = Column(Integer, nullable= False, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    link = Column(Text, nullable=True)
    year = Column(Integer, nullable=True)
    type = Column(String, nullable=False)
    sjr = Column(String, nullable=True)
    q_rank = Column(String, nullable=True)
    h_index = Column(Integer, nullable=True)
    total_docs = Column(Integer, nullable=True)
    total_docs_3years = Column(Integer, nullable=True)
    total_refs = Column(Integer, nullable=True)
    total_cites_3years = Column(Integer, nullable=True)
    citable_docs_3years = Column(Integer, nullable=True)
    cites_per_doc_2years = Column(String, nullable=True)
    refs_per_doc = Column(String, nullable=True)
    female_percent = Column(String, nullable=True)

    publications = relationship("Publication", back_populates="journal")

    def __repr__(self):
        return f"<Journal(title={self.title}, type={self.type}, h_index={self.h_index})>"
