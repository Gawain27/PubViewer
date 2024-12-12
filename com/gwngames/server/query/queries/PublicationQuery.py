from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.entity.base.Conference import Conference
from com.gwngames.server.entity.base.Journal import Journal
from com.gwngames.server.entity.base.Publication import Publication
from com.gwngames.server.entity.base.Relationships import PublicationAuthor
from com.gwngames.server.entity.variant.scholar.GoogleScholarCitation import GoogleScholarCitation
from com.gwngames.server.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication
from com.gwngames.server.query.QueryBuilder import QueryBuilder


class PublicationQuery:
    @staticmethod
    def build_specific_publication_query(session, pub_id: str):
        """
        Build a specific query for a publication, aggregating data useful for researchers.
        """
        publication_query = QueryBuilder(session, Publication, "p")
        journal_query = QueryBuilder(session, Journal, "j")
        conference_query = QueryBuilder(session, Conference, "c")
        google_scholar_query = QueryBuilder(session, GoogleScholarPublication, "gsp")
        citation_query = QueryBuilder(session, GoogleScholarCitation, "gsc")
        assoc_query = QueryBuilder(session, PublicationAuthor, "ass")
        author_query = QueryBuilder(session, Author, "a")

        # Joins
        publication_query.join("LEFT", journal_query, "j", on_condition="j.id = p.journal_id")
        publication_query.join("LEFT", conference_query, "c", on_condition="c.id = p.conference_id")
        publication_query.join("LEFT", google_scholar_query, "gsp", on_condition="gsp.id = p.id")
        publication_query.join("LEFT", citation_query, "gsc", on_condition="gsc.publication_id = gsp.id")
        publication_query.join("LEFT", assoc_query, "ass", on_condition="ass.publication_id = p.id")
        publication_query.join("LEFT", author_query, "a", on_condition="a.id = ass.author_id")

        # Filter by title
        publication_query.and_condition("", "p.id = " + pub_id, custom=True)

        # Select fields and aggregations
        publication_query.select(
            """
            p.id AS ID,
            p.title AS "Title",
            p.description AS "Description",
            p.publication_year as Year,
            p.publisher as Publisher,
            p.url AS "Scholar URL",
            STRING_AGG(DISTINCT lower(SPLIT_PART(a.name, ' ', 2)), ', ') AS Authors,
            CASE 
                WHEN COUNT(j.id) > 0 THEN MODE() WITHIN GROUP (ORDER BY REGEXP_REPLACE(j.sjr, '[^0-9.]', ''))
                ELSE '0'
            END AS "Journal Score",
            CASE 
                WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)
                ELSE '-'
            END AS "Journal Rank",
            j.title AS "Journal",
            COALESCE(j.h_index, 0) AS "Journal H-Index",
            c.acronym as "Conference",
            CASE 
                WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank)
                ELSE '-'
            END AS "Conference Rank",
            COALESCE(SUM(gsp.total_citations), 0) AS "Total Citations",
            STRING_AGG(DISTINCT gsc.title, ', ') AS "Cited Titles"
            """
        )

        # Group and limit
        publication_query.group_by(
            "p.id", "p.title", "p.description", "p.publication_year", "p.publisher", "p.url",
            "j.h_index", "c.year", "j.title", "c.acronym"
        )
        publication_query.limit(1)

        return publication_query

    @staticmethod
    def build_overview_publication_query(session):
        """
        Build an overview query for publications, including associated journal, conference, and Google Scholar data.
        """
        publication_query = QueryBuilder(session, Publication, "p")
        journal_query = QueryBuilder(session, Journal, "j")
        conference_query = QueryBuilder(session, Conference, "c")
        google_scholar_query = QueryBuilder(session, GoogleScholarPublication, "gsp")
        assoc_query = QueryBuilder(session, PublicationAuthor, "ass")
        author_query = QueryBuilder(session, Author, "a")


        publication_query.join(
            "LEFT", journal_query, "j", on_condition="j.id = p.journal_id"
        ).join(
            "LEFT", conference_query, "c", on_condition="c.id = p.conference_id"
        ).join(
            "LEFT", google_scholar_query, "gsp", on_condition="gsp.id = p.id"
        ).join(
            "LEFT", assoc_query, "ass", on_condition="ass.publication_id = p.id"
        ).join(
            "LEFT", author_query, "a", on_condition="a.id = ass.author_id"
        )

        publication_query.and_condition("",
            """
            (c.rank IS NOT NULL OR j.q_rank IS NOT NULL OR gsp.id IS NOT NULL)
            """, custom=True
        )

        publication_query.select(
            """
            p.id AS ID,
            p.title AS Title,
            p.publication_year as Year,
            p.publisher as Publisher,
            STRING_AGG(DISTINCT lower(a.name), ', ') AS Authors,
            CASE 
                WHEN COUNT(j.id) > 0 THEN MODE() WITHIN GROUP (ORDER BY REGEXP_REPLACE(j.sjr, '[^0-9.]', ''))
                ELSE '0'
            END AS "Journal Score",
            CASE 
                WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)
                ELSE '-'
            END AS "Journal Rank",
            CASE 
                WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank)
                ELSE '-'
            END AS "Conference Rank"
            """
        )

        publication_query.group_by(
            "p.id", "p.title", "p.publication_year", "p.publisher"
        )

        return publication_query





