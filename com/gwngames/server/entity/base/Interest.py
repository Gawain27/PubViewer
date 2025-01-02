from sqlalchemy import Column, Integer, String

from com.gwngames.server.entity.base.BaseEntity import BaseEntity


class Interest(BaseEntity):
    """
    Represents an interest associated with authors.
    """
    __tablename__ = 'interest'

    CLASS_ID = 1002
    VARIANT_ID = 1

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)

    def __repr__(self):
        return f"<Interest(name={self.name})>"
