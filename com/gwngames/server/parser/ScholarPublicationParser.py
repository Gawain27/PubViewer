from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.entity.base.Publication import Publication
from com.gwngames.server.entity.base.Relationships import PublicationAuthor
from com.gwngames.server.entity.variant.scholar.GoogleScholarCitation import GoogleScholarCitation
from com.gwngames.server.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication


class ScholarPublicationParser:
    """
    Processes and persists Google Scholar publication data, including citations, authors, and metadata.
    """

    def __init__(self, session: Session):
        self.session = session

    def process_json(self, json_data: dict):
        """
        Processes the provided JSON and persists/updates the publication and related data.
        """
        try:
            self.session.begin_nested()

            publication = self._process_publication(json_data)

            gscholar_publication = self._process_google_scholar_publication(json_data, publication)

            self._process_authors(json_data, publication)

            self._process_citations(json_data, gscholar_publication)

            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise Exception(f"Error processing JSON data: {str(e)}")

    def _process_publication(self, json_data: dict) -> Publication:
        """
        Processes the Publication entity.
        """
        publication = (
            self.session.query(Publication)
            .filter(Publication.title == json_data["title"])
            .with_for_update()
            .first()
        )

        if not publication:
            publication = Publication(
                title=json_data["title"],
                url=json_data.get("publication_url"),
                publication_date=json_data.get("publication_date"),
                pages=json_data.get("pages"),
                publisher=json_data.get("publisher"),
                description=json_data.get("description"),
            )
            self.session.add(publication)

        # Update BaseEntity metadata
        publication.update_date = json_data.get("update_date")
        publication.update_count = json_data.get("update_count", publication.update_count + 1)

        return publication

    def _process_google_scholar_publication(self, json_data: dict, publication: Publication) -> GoogleScholarPublication:
        """
        Processes the Google Scholar-specific publication data.
        """
        gscholar_publication = (
            self.session.query(GoogleScholarPublication)
            .filter(GoogleScholarPublication.publication_id == json_data["publication_id"])
            .with_for_update()
            .first()
        )

        if not gscholar_publication:
            gscholar_publication = GoogleScholarPublication(
                publication_id=json_data["publication_id"],
                title_link=json_data.get("title_link"),
                pdf_link=json_data.get("pdf_link"),
                total_citations=json_data.get("total_citations"),
                cites_id=json_data.get("cites_id"),
                related_articles_url=json_data.get("related_articles_url"),
                all_versions_url=json_data.get("all_versions_url"),
            )
            gscholar_publication.publication = publication
            self.session.add(gscholar_publication)

        # Update BaseEntity metadata
        gscholar_publication.id = publication.id
        gscholar_publication.update_date = json_data.get("update_date")
        gscholar_publication.update_count = json_data.get("update_count", gscholar_publication.update_count + 1)

        return gscholar_publication

    def _process_authors(self, json_data: dict, publication: Publication):
        """
        Processes and associates authors with the publication.
        """
        authors = json_data.get("authors", [])
        for author_name in authors:
            # Fetch or create the author
            author = (
                self.session.query(Author)
                .filter(Author.name == author_name)
                .with_for_update()
                .first()
            )

            if not author:
                author = Author(name=author_name)
                self.session.add(author)
                self.session.flush()  # Flush to get the `id` assigned

            # Check if the association already exists
            association_exists = (
                self.session.query(PublicationAuthor)
                .filter_by(publication_id=publication.id, author_id=author.id)
                .first()
            )

            if not association_exists:
                # Add the association explicitly
                association = PublicationAuthor(publication_id=publication.id, author_id=author.id)
                self.session.add(association)

    def _process_citations(self, json_data: dict, gscholar_publication: GoogleScholarPublication):
        """
        Processes and associates citations with the Google Scholar publication.
        """
        citations = json_data.get("citation_graph", [])
        for citation_data in citations:
            citation_id = citation_data["citation_link"]
            citation = (
                self.session.query(GoogleScholarCitation)
                .filter(GoogleScholarCitation.citation_link == citation_id)
                .with_for_update()
                .first()
            )

            if not citation:
                citation = GoogleScholarCitation(
                    publication_id=gscholar_publication.id,
                    citation_link=citation_id,
                    year=citation_data["year"],
                    citations=citation_data["citations"],
                    title=gscholar_publication.publication.title,
                    link=gscholar_publication.title_link,
                    summary=gscholar_publication.publication.description,
                )
                self.session.add(citation)

            # Update BaseEntity metadata
            citation.update_date = json_data.get("update_date")
            citation.update_count = json_data.get("update_count", citation.update_count + 1)