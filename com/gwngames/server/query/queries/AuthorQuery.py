from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.entity.base.Conference import Conference
from com.gwngames.server.entity.base.Interest import Interest
from com.gwngames.server.entity.base.Journal import Journal
from com.gwngames.server.entity.base.Publication import Publication
from com.gwngames.server.entity.base.Relationships import PublicationAuthor, AuthorInterest, AuthorCoauthor
from com.gwngames.server.entity.variant.scholar.GoogleScholarAuthor import GoogleScholarAuthor
from com.gwngames.server.query.QueryBuilder import QueryBuilder


class AuthorQuery:

    @staticmethod
    def build_author_query_with_filter(session, author_id: int):
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
            "LEFT", scholar_query, "g", on_condition="a.id = g.author_key"
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
        author_query.and_condition("a.id", author_id)

        # Select required fields and aggregations
        author_query.select(
            """
            a.id AS "Author ID",
            a.name AS "Name",
            a.role as Role,
            a.organization as Organization,
            a.image_url as "Image url",
            a.homepage_url as "Homepage",
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
                WHEN COUNT(j.sjr) > 0 THEN AVG(
                    CAST(
                        COALESCE(NULLIF(REGEXP_REPLACE(j.sjr, '[^0-9.]', ''), ''), '0') AS FLOAT
                    )
                )
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
            "LEFT", google_scholar_query, "gsa", on_condition="gsa.author_key = a.id"
        )

        # Select required fields and aggregations with significant aliases
        author_query.select(
            """
            a.id AS "Author ID",
            a.name AS Name,
            a.role AS Role,
            a.organization AS Organization,
            a.image_url as "Image url",
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
                WHEN COUNT(j.sjr) > 0 THEN AVG(
                    CAST(
                        COALESCE(NULLIF(REGEXP_REPLACE(j.sjr, '[^0-9.]', ''), ''), '0') AS FLOAT
                    )
                )
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
                gsa.author_id IS NOT NULL
            )
            """,
            custom=True
        )

        return author_query


    @staticmethod
    def build_author_group_query(session, author_id):
        query_builder = QueryBuilder(session, Publication, "p")

        # Join necessary tables
        query_builder.join(
            "INNER", PublicationAuthor, "pa_start", on_condition="p.id = pa_start.publication_id"
        ).join(
            "INNER", Author, "start_author", on_condition="pa_start.author_id = start_author.id"
        ).join(
            "INNER", GoogleScholarAuthor, "start_gs", on_condition="start_author.id = start_gs.author_key"
        ).join(
            "INNER", PublicationAuthor, "pa_end", on_condition="p.id = pa_end.publication_id"
        ).join(
            "INNER", Author, "end_author", on_condition="pa_end.author_id = end_author.id"
        ).join(
            "INNER", GoogleScholarAuthor, "end_gs", on_condition="end_author.id = end_gs.author_key"
        ).join(
            "LEFT", Conference, "c", on_condition="p.conference_id = c.id"
        ).join(
            "LEFT", Journal, "j", on_condition="p.journal_id = j.id"
        )

        # Add filters
        query_builder.and_condition("start_author.id", author_id)
        query_builder.and_condition("", "(j.q_rank IS NOT NULL or c.rank IS NOT NULL)", custom=True)

        # Add SELECT clause
        query_builder.select(
            """
            start_author.id AS start_author_id,
            start_author.name AS start_author_label,
            start_author.image_url AS start_author_image_url,
            CONCAT(start_author.role, ' ', start_author.organization) AS start_author_info,
            end_author.id AS end_author_id,
            end_author.name AS end_author_label,
            end_author.image_url AS end_author_image_url,
            CONCAT(end_author.role, ' ', end_author.organization) AS end_author_info,
            COUNT(p.id) AS author_total_pubs,
            MODE() WITHIN GROUP (ORDER BY c.rank) AS avg_conference_rank,
            MODE() WITHIN GROUP (ORDER BY j.q_rank) AS avg_journal_rank
            """
        )

        # Group by necessary columns
        query_builder.group_by(
            "start_author.id",
            "start_author.name",
            "start_author.image_url",
            "CONCAT(start_author.role, ' ', start_author.organization)",
            "end_author.id",
            "end_author.name",
            "end_author.image_url",
            "CONCAT(end_author.role, ' ', end_author.organization)"
        )

        return query_builder


