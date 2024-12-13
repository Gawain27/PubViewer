from sqlalchemy import Table, Column, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Define Association Models
class AuthorCoauthor(Base):
    __tablename__ = "author_coauthor"
    author_id = Column(Integer, primary_key=True)
    coauthor_id = Column(Integer, primary_key=True)

class AuthorInterest(Base):
    __tablename__ = "author_interest"
    author_id = Column(Integer, primary_key=True)
    interest_id = Column(Integer, primary_key=True)

class PublicationAuthor(Base):
    __tablename__ = "publication_author"
    publication_id = Column(Integer, primary_key=True)
    author_id = Column(Integer, primary_key=True)