from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.entity.base.Conference import Conference
from com.gwngames.server.entity.base.Interest import Interest
from com.gwngames.server.entity.base.Journal import Journal
from com.gwngames.server.entity.base.Publication import Publication
from com.gwngames.server.entity.base.Relationships import PublicationAuthor, AuthorInterest
from com.gwngames.server.entity.variant.scholar.GoogleScholarAuthor import GoogleScholarAuthor
from com.gwngames.server.query.QueryBuilder import QueryBuilder


class AuthorQuery:

    @staticmethod
    def build_author_query_with_filter(session, author_name: str):
        # Base QueryBuilder for Author
        author_query = QueryBuilder(session, Author, "a")
        scholar_query = QueryBuilder(session, GoogleScholarAuthor, "g")
        publication_author_query = QueryBuilder(session, PublicationAuthor, "pa")
        publication_query = QueryBuilder(session, Publication, "p")
        journal_query = QueryBuilder(session, Journal, "j")
        conference_query = QueryBuilder(session, Conference, "c")
        author_interest_query = QueryBuilder(session, AuthorInterest, "ai")
        interest_query = QueryBuilder(session, Interest, "i")

        # Joins for relations
        author_query.join(
            "LEFT", scholar_query, "g", on_condition="a.id = g.id"
        ).join(
            "LEFT", publication_author_query, "pa", on_condition="a.id = pa.author_id"
        ).join(
            "LEFT", publication_query, "p", on_condition="p.id = pa.publication_id"
        ).join(
            "LEFT", journal_query, "j", on_condition="j.id = p.journal_id"
        ).join(
            "LEFT", conference_query, "c", on_condition="c.id = p.conference_id"
        ).join(
            "LEFT", author_interest_query, "ai", on_condition="a.id = ai.author_id"
        ).join(
            "LEFT", interest_query, "i", on_condition="i.id = ai.interest_id"
        )

        # Add WHERE condition for author name
        author_query.and_condition("a.name", author_name)

        # Select required fields and aggregations
        author_query.select(
            """
            a.id AS "Author ID",
            a.name AS Name,
            a.role as Role,
            a.organization as Organization,
            a.image_url as "Image url",
            a.homepage_url as Homepage,
            COALESCE(g.author_id, 'N/A') AS "Scholar ID",
            COALESCE(g.profile_url, 'N/A') AS "Scholar Profile",
            COALESCE(g.verified, 'N/A') AS "Verified on",
            COALESCE(g.h_index, 0) AS "H Index",
            COALESCE(g.i10_index, 0) AS "I10 Index",
            COALESCE(STRING_AGG(DISTINCT i.name, ', '), 'N/A') AS Interests,
            CASE 
                WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank)
                ELSE 'N/A'
            END AS "Avg. Conf. Rank",
            CASE 
                WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)
                ELSE 'N/A'
            END AS "Avg. Journal Rank",
            CASE 
                WHEN COUNT(j.sjr) > 0 THEN AVG(CAST(REGEXP_REPLACE(j.sjr, '[^0-9.]', '') AS FLOAT))
                ELSE 0
            END AS "Avg. SJR Score"
            """
        )

        # Group by author and scholar-related fields
        author_query.group_by(
            "a.id", "g.author_id", "a.role", "a.organization", "a.image_url", "a.homepage_url",
            "g.profile_url", "g.verified", "g.h_index", "g.i10_index"
        )

        author_query.limit(1)

        return author_query

    @staticmethod
    def build_author_overview_query(session):
        # Base QueryBuilder for Author
        author_query = QueryBuilder(session, Author, "a")
        publication_author_query = QueryBuilder(session, PublicationAuthor, "pa")
        publication_query = QueryBuilder(session, Publication, "p")
        journal_query = QueryBuilder(session, Journal, "j")
        conference_query = QueryBuilder(session, Conference, "c")
        author_interest_query = QueryBuilder(session, AuthorInterest, "ai")
        interest_query = QueryBuilder(session, Interest, "i")
        google_scholar_query = QueryBuilder(session, GoogleScholarAuthor, "gsa")

        # Joins for relations
        author_query.join(
            "LEFT", publication_author_query, "pa", on_condition="a.id = pa.author_id"
        ).join(
            "LEFT", publication_query, "p", on_condition="p.id = pa.publication_id"
        ).join(
            "LEFT", journal_query, "j", on_condition="j.id = p.journal_id"
        ).join(
            "LEFT", conference_query, "c", on_condition="c.id = p.conference_id"
        ).join(
            "LEFT", author_interest_query, "ai", on_condition="a.id = ai.author_id"
        ).join(
            "LEFT", interest_query, "i", on_condition="i.id = ai.interest_id"
        ).join(
            "LEFT", google_scholar_query, "gsa", on_condition="gsa.id = a.id"
        )

        # Select required fields and aggregations with significant aliases
        author_query.select(
            """
            a.id AS "Author ID",
            a.name AS Name,
            a.role AS Role,
            a.organization AS Organization,
            COALESCE(STRING_AGG(DISTINCT i.name, ', '), 'N/A') AS Interests,
            CASE 
                WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank)
                ELSE '-'
            END AS "Avg. Conf. Rank",
            CASE 
                WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)
                ELSE '-'
            END AS "Avg. Journal Rank",
            CASE 
                WHEN COUNT(j.sjr) > 0 THEN AVG(CAST(REGEXP_REPLACE(j.sjr, '[^0-9.]', '') AS FLOAT))
                ELSE 0
            END AS "Avg. SJR Score"
            """
        )

        # Adding conditions to filter records
        author_query.group_by(
            "a.id", "a.role", "a.organization", "gsa.author_id"
        ).having_and("", value=
            """
            (
                COUNT(c.rank) > 0 OR
                COUNT(j.q_rank) > 0 OR
                gsa.author_id IS NOT NULL
            )
            """,
            custom=True
        )

        return author_query

