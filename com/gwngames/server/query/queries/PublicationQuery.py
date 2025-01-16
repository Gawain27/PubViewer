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

        # Joins
        publication_query.join("LEFT", journal_query, "j", on_condition="j.id = p.journal_id")
        publication_query.join("LEFT", conference_query, "c", on_condition="c.id = p.conference_id")
        publication_query.join("LEFT", google_scholar_query, "gsp", on_condition="gsp.publication_key = p.id")
        publication_query.join("LEFT", citation_query, "gsc", on_condition="gsc.publication_id = gsp.id")

        publication_query.and_condition("", "p.id = " + pub_id, custom=True)

        publication_query.select(
            """
            p.id AS "Pub. ID",
            to_camel_case(p.title) AS "Title",
            p.description AS "Description",
            CASE
                WHEN p.publication_year < 1950 THEN '-'
                ELSE p.publication_year || ''
            END AS "Year",
            p.publisher as "Publisher",
            p.url AS "Scholar URL",
            p.authors AS "Authors",
            CASE 
                WHEN COUNT(j.id) > 0 THEN MODE() WITHIN GROUP (
                    ORDER BY COALESCE(NULLIF(REGEXP_REPLACE(j.sjr, '[^0-9.]', ''), ''), '0')
                )
                ELSE '0'
            END AS "Journal Score",
            CASE WHEN j.q_rank IS NULL THEN 'N/A' ELSE j.q_rank END AS "Journal Rank",
            to_camel_case(j.title) AS "Journal",
            COALESCE(j.h_index, 0) AS "Journal H-Index",
            c.acronym as "Conference",
            CASE WHEN c.rank IS NULL THEN 'N/A' ELSE c.rank END AS "Conference Rank",
            COALESCE(SUM(gsp.total_citations), 0) AS "Total Citations"
            """
        )

        publication_query.group_by(
            "p.id", "p.title", "p.description", "p.publication_year", "p.publisher", "p.url",
            "j.h_index", "c.year", "j.title", "c.acronym, c.rank, j.q_rank"
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

        publication_query.join(
            "LEFT", journal_query, "j", on_condition="j.id = p.journal_id"
        ).join(
            "LEFT", conference_query, "c", on_condition="c.id = p.conference_id"
        ).join(
            "LEFT", google_scholar_query, "gsp", on_condition="gsp.publication_key = p.id"
        )

        publication_query.and_condition("",
            """
            (gsp.id IS NOT NULL)
            """, custom=True
        )

        publication_query.select(
            """
            '' || p.id AS "ID",
            to_camel_case(p.title) AS "Title",
            CASE
                WHEN p.publication_year < 1950 THEN ''
                ELSE p.publication_year || ''
            END as "Year",
            p.publisher as "Publisher",
            p.authors AS "Authors",
            '' || CASE 
                WHEN COUNT(j.id) > 0 THEN MODE() WITHIN GROUP (
                    ORDER BY COALESCE(NULLIF(REGEXP_REPLACE(j.sjr, '[^0-9.]', ''), ''), '0')
                )
                ELSE '0'
            END AS "Journal Score",
            j.q_rank AS "Journal Rank",
            c.rank AS "Conference Rank"
            """
        )

        publication_query.group_by(
            "p.id", "p.title", "p.publication_year", "p.publisher, j.q_rank, c.rank"
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
        #qb.and_condition("", f"(pa1.author_id, pa2.author_id) IN ({pairs})", custom=True)
        qb.join("INNER", f"(VALUES {pairs})", "pair(id1, id2)", on_condition="(pa1.author_id, pa2.author_id) = (pair.id1, pair.id2) AND pair.id1 < pair.id2")
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
        #qb.and_condition("",f"(pa1.author_id, pa2.author_id) IN ({pairs})", custom=True)
        qb.join("INNER", f"(VALUES {pairs})", "pair(id1, id2)", on_condition="(pa1.author_id, pa2.author_id) = (pair.id1, pair.id2) AND pair.id1 < pair.id2")
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





