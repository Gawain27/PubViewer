from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Import Models and Relationships
from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.entity.base.Conference import Conference
from com.gwngames.server.entity.base.Interest import Interest
from com.gwngames.server.entity.base.Journal import Journal
from com.gwngames.server.entity.base.Publication import Publication
from com.gwngames.server.entity.base.Relationships import AuthorCoauthor, PublicationAuthor, AuthorInterest
from com.gwngames.server.entity.variant.scholar.GoogleScholarAuthor import GoogleScholarAuthor
from com.gwngames.server.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication
from com.gwngames.server.entity.variant.scholar.GoogleScholarCitation import GoogleScholarCitation

# Database Configuration
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create All Tables Dynamically
def create_all_tables():
    """
    Bind and create all models and associations dynamically.
    """
    print("Creating tables...")

    # Get all the models
    models = [
        Author,
        Conference,
        Interest,
        Journal,
        Publication,
        GoogleScholarAuthor,
        GoogleScholarPublication,
        GoogleScholarCitation,
    ]

    # Bind each model to the engine and create tables
    for model in models:
        print(f"Creating tables for {model.__name__}...")
        model.metadata.create_all(bind=engine)

    # Create tables for the association tables explicitly
    print("Creating association tables...")
    AuthorCoauthor.metadata.create_all(bind=engine, checkfirst=True)
    PublicationAuthor.metadata.create_all(bind=engine, checkfirst=True)
    AuthorInterest.metadata.create_all(bind=engine, checkfirst=True)
    GoogleScholarCitation.negative_seq.metadata.create_all(bind=engine)

    print("All tables created successfully!")

# Entry Point
if __name__ == "__main__":
    # Create all tables
    create_all_tables()

    # Initialize a database session for testing
    session = SessionLocal()

    # Example of adding a new Author (for testing purposes)
    try:
        new_author = Author(name="John Doe", class_id=1, variant_id=1)
        session.add(new_author)
        session.commit()
        print("Added a new author to the database.")
    except Exception as e:
        print(f"Error adding author: {e}")
        session.rollback()
    finally:
        session.close()
