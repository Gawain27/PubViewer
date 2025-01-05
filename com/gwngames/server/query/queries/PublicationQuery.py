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
        publication_query = QueryBuilder(session, Publication.__tablename__, "p")
        journal_query = QueryBuilder(session, Journal.__tablename__, "j")
        conference_query = QueryBuilder(session, Conference.__tablename__, "c")
        google_scholar_query = QueryBuilder(session, GoogleScholarPublication.__tablename__, "gsp")
        citation_query = QueryBuilder(session, GoogleScholarCitation.__tablename__, "gsc")
        assoc_query = QueryBuilder(session, PublicationAuthor.__tablename__, "ass")
        author_query = QueryBuilder(session, Author.__tablename__, "a")

        # Joins
        publication_query.join("LEFT", journal_query, "j", on_condition="j.id = p.journal_id")
        publication_query.join("LEFT", conference_query, "c", on_condition="c.id = p.conference_id")
        publication_query.join("LEFT", google_scholar_query, "gsp", on_condition="gsp.publication_key = p.id")
        publication_query.join("LEFT", citation_query, "gsc", on_condition="gsc.publication_id = gsp.id")
        publication_query.join("LEFT", assoc_query, "ass", on_condition="ass.publication_id = p.id")
        publication_query.join("LEFT", author_query, "a", on_condition="a.id = ass.author_id")

        # Filter by title
        publication_query.and_condition("", "p.id = " + pub_id, custom=True)

        # Select fields and aggregations
        publication_query.select(
            """
            p.id AS "Pub. ID",
            p.title AS "Title",
            p.description AS "Description",
            p.publication_year as Year,
            p.publisher as Publisher,
            p.url AS "Scholar URL",
            STRING_AGG(DISTINCT a.name, ', ') AS Authors,
            CASE 
                WHEN COUNT(j.id) > 0 THEN MODE() WITHIN GROUP (
                    ORDER BY COALESCE(NULLIF(REGEXP_REPLACE(j.sjr, '[^0-9.]', ''), ''), '0')
                )
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
        publication_query = QueryBuilder(session, Publication.__tablename__, "p")
        journal_query = QueryBuilder(session, Journal.__tablename__, "j")
        conference_query = QueryBuilder(session, Conference.__tablename__, "c")
        google_scholar_query = QueryBuilder(session, GoogleScholarPublication.__tablename__, "gsp")
        assoc_query = QueryBuilder(session, PublicationAuthor.__tablename__, "ass")
        author_query = QueryBuilder(session, Author.__tablename__, "a")


        publication_query.join(
            "LEFT", journal_query, "j", on_condition="j.id = p.journal_id"
        ).join(
            "LEFT", conference_query, "c", on_condition="c.id = p.conference_id"
        ).join(
            "LEFT", google_scholar_query, "gsp", on_condition="gsp.publication_key = p.id"
        ).join(
            "LEFT", assoc_query, "ass", on_condition="ass.publication_id = p.id"
        ).join(
            "LEFT", author_query, "a", on_condition="a.id = ass.author_id"
        )

        publication_query.and_condition("",
            """
            (gsp.id IS NOT NULL)
            """, custom=True
        )

        publication_query.select(
            """
            DISTINCT p.id AS ID,
            p.title AS Title,
            p.publication_year as Year,
            p.publisher as Publisher,
            STRING_AGG(DISTINCT lower(a.name), ', ') AS Authors,
            CASE 
                WHEN COUNT(j.id) > 0 THEN MODE() WITHIN GROUP (
                    ORDER BY COALESCE(NULLIF(REGEXP_REPLACE(j.sjr, '[^0-9.]', ''), ''), '0')
                )
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

    @staticmethod
    def build_author_publication_query_batch(session, pairs):
        pairs = ','.join(str(pair) for pair in pairs)
        qb = QueryBuilder(session, Publication.__tablename__, "p")
        qb.join(
            "INNER", PublicationAuthor.__tablename__, "pa1", "p.id = pa1.publication_id"
        ).join(
            "INNER", PublicationAuthor.__tablename__, "pa2", "p.id = pa2.publication_id"
        ).join(
            "LEFT", Journal.__tablename__, "j", "p.journal_id = j.id"
        ).join(
            "LEFT", Conference.__tablename__, "c", "p.conference_id = c.id"
        )
        qb.and_condition("", "(j.q_rank IS NOT NULL OR c.rank IS NOT NULL)", custom=True)
        qb.and_condition("", f"(pa1.author_id, pa2.author_id) IN ({pairs})", custom=True)
        qb.select(
            """
            pa1.author_id AS aid1,
            pa2.author_id AS aid2,
            COALESCE(j.q_rank, c.rank) AS rank_name,
            COUNT(p.id) AS rank_total_pubs
            """
        )
        qb.group_by("pa1.author_id", "pa2.author_id", "rank_name")
        return qb

    @staticmethod
    def build_author_publication_year_query_batch(session, pairs):
        pairs = ','.join(str(pair) for pair in pairs)
        qb = QueryBuilder(session, Publication.__tablename__, "p")
        qb.join(
            "INNER", PublicationAuthor.__tablename__, "pa1", "p.id = pa1.publication_id"
        ).join(
            "INNER", PublicationAuthor.__tablename__, "pa2", "p.id = pa2.publication_id"
        )
        qb.and_condition("", "(p.journal_id IS NOT NULL OR p.conference_id IS NOT NULL)", custom=True)
        qb.and_condition("", "p.publication_year IS NOT NULL", custom=True)
        qb.and_condition("",f"(pa1.author_id, pa2.author_id) IN ({pairs})", custom=True)
        qb.select(
            """
            pa1.author_id AS aid1,
            pa2.author_id AS aid2,
            p.publication_year AS publication_year,
            COUNT(p.id) AS publication_count
            """
        )
        qb.group_by("pa1.author_id", "pa2.author_id", "p.publication_year")
        return qb




