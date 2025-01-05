from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.entity.base.Conference import Conference
from com.gwngames.server.entity.base.Interest import Interest
from com.gwngames.server.entity.base.Journal import Journal
from com.gwngames.server.entity.base.Publication import Publication
from com.gwngames.server.entity.base.Relationships import PublicationAuthor, AuthorInterest
from com.gwngames.server.entity.variant.scholar.GoogleScholarAuthor import GoogleScholarAuthor
from com.gwngames.server.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication
from com.gwngames.server.query.QueryBuilder import QueryBuilder


class AuthorQuery:

    @staticmethod
    def build_author_query_with_filter(session, author_id: int):
        # Base QueryBuilder for Author
        author_query = QueryBuilder(session, Author.__tablename__, "a")
        scholar_query = QueryBuilder(session, GoogleScholarAuthor.__tablename__, "g")
        publication_author_query = QueryBuilder(session, PublicationAuthor.__tablename__, "pa")
        publication_query = QueryBuilder(session, Publication.__tablename__, "p")
        journal_query = QueryBuilder(session, Journal.__tablename__, "j")
        conference_query = QueryBuilder(session, Conference.__tablename__, "c")
        author_interest_query = QueryBuilder(session, AuthorInterest.__tablename__, "ai")
        interest_query = QueryBuilder(session, Interest.__tablename__, "i")
        gsp_query = QueryBuilder(session, GoogleScholarPublication.__tablename__, "gsp")

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
        ).join(
            # Join with Google Scholar Publication
            "LEFT", gsp_query, "gsp", on_condition="gsp.publication_key = p.id"
        )

        # Add WHERE condition for author id
        author_query.and_condition("a.id", author_id)

        # Select required fields and aggregations
        author_query.select(
            """
            a.id AS "Author ID",
            a.name AS "Name",
            a.role as "Role",
            a.organization as "Organization",
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
            END AS "Avg. SJR Score",

            -- NEW FIELDS:
            COALESCE(SUM(gsp.total_citations), 0) AS "Total Cites",
            COALESCE(COUNT(DISTINCT gsp.id), 0) AS "Publications Found"
            """
        )

        # Group by author and scholar-related fields (excluding aggregated columns)
        author_query.group_by(
            "a.id",
            "g.author_id",
            "a.role",
            "a.organization",
            "a.image_url",
            "a.homepage_url",
            "g.profile_url",
            "g.verified",
            "g.h_index",
            "g.i10_index"
        )

        author_query.limit(1)

        return author_query

    @staticmethod
    def build_author_overview_query(session):
        # Base QueryBuilder for Author
        author_query = QueryBuilder(session, Author.__tablename__, "a")
        publication_author_query = QueryBuilder(session, PublicationAuthor.__tablename__, "pa")
        publication_query = QueryBuilder(session, Publication.__tablename__, "p")
        journal_query = QueryBuilder(session, Journal.__tablename__, "j")
        conference_query = QueryBuilder(session, Conference.__tablename__, "c")
        author_interest_query = QueryBuilder(session, AuthorInterest.__tablename__, "ai")
        interest_query = QueryBuilder(session, Interest.__tablename__, "i")
        google_scholar_query = QueryBuilder(session, GoogleScholarAuthor.__tablename__, "gsa")

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
            DISTINCT a.id AS "Author ID",
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
    def build_author_group_query_batch(session, author_ids):
        author_ids = ",".join(map(str, author_ids))

        qb = QueryBuilder(session, Publication.__tablename__, "p")
        qb.join(
            "INNER", PublicationAuthor.__tablename__, "pa_start", "p.id = pa_start.publication_id"
        ).join(
            "INNER", Author.__tablename__, "start_author", "pa_start.author_id = start_author.id"
        ).join(
            "INNER", GoogleScholarAuthor.__tablename__, "start_gs", "start_author.id = start_gs.author_key"
        ).join(
            "INNER", PublicationAuthor.__tablename__, "pa_end", "p.id = pa_end.publication_id"
        ).join(
            "INNER", Author.__tablename__, "end_author", "pa_end.author_id = end_author.id"
        ).join(
            "INNER", GoogleScholarAuthor.__tablename__, "end_gs", "end_author.id = end_gs.author_key"
        )
        qb.and_condition("", f"start_author.id IN ({author_ids})", custom=True)
        qb.select(
            """
            start_author.id AS start_author_id,
            start_author.name AS start_author_label,
            start_author.image_url AS start_author_image_url,
            end_author.id AS end_author_id,
            end_author.name AS end_author_label,
            end_author.image_url AS end_author_image_url
            """
        )
        qb.group_by(
            "start_author.id", "start_author.name", "start_author.image_url",
            "end_author.id", "end_author.name", "end_author.image_url"
        )
        return qb


