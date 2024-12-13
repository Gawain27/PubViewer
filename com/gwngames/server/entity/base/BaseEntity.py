from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, TIMESTAMP, func
from sqlalchemy.orm import declared_attr

Base = declarative_base()

class BaseEntity(Base):
    """
    Base class for all entities, representing the T_BASE_ENTITY table.
    """
    __abstract__ = True

    id = Column(Integer, primary_key=True, nullable=False)
    class_id = Column(Integer, nullable=False)
    variant_id = Column(Integer, nullable=False)

    creation_date = Column(TIMESTAMP, nullable=False, server_default=func.now())
    update_date = Column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())
    update_count = Column(Integer, nullable=False, default=1)

    @declared_attr
    def CLASS_ID(cls):
        raise NotImplementedError("Subclasses must define CLASS_ID.")

    @declared_attr
    def VARIANT_ID(cls):
        raise NotImplementedError("Subclasses must define VARIANT_ID.")

    def __repr__(self):
        return f"<{self.__class__.__name__}(id={self.id}, class_id={self.class_id}, variant_id={self.variant_id})>"