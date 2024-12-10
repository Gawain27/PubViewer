from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declared_attr

from com.gwngames.server.entity.base.BaseEntity import BaseEntity


class Author(BaseEntity):
    """
    Represents an author entity.
    """
    __tablename__ = 'author'

    CLASS_ID = 1000
    VARIANT_ID = 1

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    role = Column(String)
    organization = Column(String)
    image_url = Column(Text)
    homepage_url = Column(Text)

    def __repr__(self):
        return f"<Author(name={self.name}, id={self.id}, organization={self.organization})>"
