from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from com.gwngames.server.entity.variant.scholar.GoogleScholarCitation import GoogleScholarCitation
from com.gwngames.server.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication


class ScholarCitationParser:
    """
    Processes and persists Google Scholar citation data, linking it to existing publications.
    """

    def __init__(self, session: Session):
        self.session = session

    def process_json(self, json_data: dict):
        """
        Processes the provided JSON and persists/updates citations linked to publications.
        """
        try:
            # Begin a nested transaction for pessimistic locking
            self.session.begin_nested()

            # Get the cites_id and related citations
            cites_id = json_data["_id"]
            citations = json_data.get("citations", [])

            # Find the linked publication using cites_id
            publication = self._find_publication(cites_id)
            if not publication:
                gscholar_pub = GoogleScholarPublication(
                    cites_id=cites_id,
                    class_id=GoogleScholarPublication.CLASS_ID,
                    variant_id=GoogleScholarPublication.VARIANT_ID,
                )
                self.session.add(gscholar_pub)


            # Process each citation
            for citation_data in citations:
                self._process_citation(citation_data, publication)

            # Commit the changes
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise Exception(f"Error processing JSON data: {str(e)}")

    def _find_publication(self, cites_id: str) -> GoogleScholarPublication:
        """
        Finds the publication linked to the given cites_id.
        """
        return (
            self.session.query(GoogleScholarPublication)
            .filter(GoogleScholarPublication.cites_id == cites_id)
            .with_for_update()
            .first()
        )

    def _process_citation(self, citation_data: dict, publication: GoogleScholarPublication):
        """
        Processes and persists a single citation.
        """
        citation_id = citation_data["link"]
        citation = (
            self.session.query(GoogleScholarCitation)
            .filter(GoogleScholarCitation.citation_link == citation_id)
            .with_for_update()
            .first()
        )

        if not citation:
            # Create a new citation
            citation = GoogleScholarCitation(
                publication_id=publication.id,
                citation_link=citation_id,
                cites_id=citation_data["cites_id"],
                title=citation_data.get("title"),
                link=citation_data.get("link"),
                summary=citation_data.get("summary"),
                document_link=citation_data.get("document_link"),
                year=str(publication.publication.publication_date.year),  # Assumes publication_date is a datetime.date
                citations=publication.total_citations,  # Update from publication
            )
            self.session.add(citation)
        else:
            # Update existing citation
            citation.title = citation_data.get("title", citation.title)
            citation.link = citation_data.get("link", citation.link)
            citation.summary = citation_data.get("summary", citation.summary)
            citation.document_link = citation_data.get("document_link", citation.document_link)
            citation.year = citation.year or publication.publication.publication_date.year
            citation.citations = publication.total_citations

        # Update BaseEntity metadata
        citation.update_date = citation_data.get("update_date")
        citation.update_count = citation_data.get("update_count", citation.update_count + 1)

