from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.entity.base.Conference import Conference
from com.gwngames.server.entity.base.Interest import Interest
from com.gwngames.server.entity.base.Journal import Journal
from com.gwngames.server.entity.base.Publication import Publication
from com.gwngames.server.entity.base.Relationships import PublicationAuthor, AuthorInterest, AuthorCoauthor
from com.gwngames.server.entity.variant.scholar.GoogleScholarAuthor import GoogleScholarAuthor
from com.gwngames.server.query.QueryBuilder import QueryBuilder
from com.gwngames.server.query.QueryBuilderWithCTE import RecursiveQueryBuilder


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

    @staticmethod
    def build_author_network_query(session, start_author_id, max_depth=5):
        """
        Builds a query using RecursiveQueryBuilder to fetch author relationships dynamically,
        including the total publications shared between two authors.

        :param session: SQLAlchemy session object.
        :param start_author_id: The ID of the starting author.
        :param max_depth: Maximum depth for traversal.
        :return: RecursiveQueryBuilder instance representing the query.
        """
        # Base case: Start with direct coauthors of the starting author
        base_query_builder = QueryBuilder(session, Author, "a")
        base_query_builder.select(
            "a.id AS start_author_id, a.name AS start_author_label, a.image_url AS start_author_image_url, "
            "c.id AS end_author_id, c.name AS end_author_label, c.image_url AS end_author_image_url, "
            "ARRAY[a.id] AS path, 1 AS depth"
        )
        base_query_builder.join("INNER", AuthorCoauthor, "ac", "a.id = ac.author_id")
        base_query_builder.join("INNER", Author, "c", "c.id = ac.coauthor_id")
        base_query_builder.and_condition("a.id", start_author_id)
        base_query_string = base_query_builder.build_query_string()

        # Recursive case: Extend the network based on relationships
        recursive_query = (
            "SELECT "
            "cte.end_author_id AS start_author_id, cte.end_author_label AS start_author_label, "
            "cte.end_author_image_url AS start_author_image_url, "
            "c.id AS end_author_id, c.name AS end_author_label, c.image_url AS end_author_image_url, "
            "cte.path || c.id AS path, cte.depth + 1 AS depth "
            "FROM AuthorCTE AS cte "
            "JOIN author_coauthor AS ac ON cte.end_author_id = ac.author_id "
            "JOIN author AS c ON c.id = ac.coauthor_id "
            f"WHERE cte.depth < {max_depth} AND c.id != ALL(cte.path)"
        )

        # Recursive QueryBuilder setup
        query_builder = RecursiveQueryBuilder(session, None, None)
        query_builder.add_cte("AuthorCTE", f"{base_query_string} UNION ALL {recursive_query}")

        # Final SELECT with total publications
        query_builder.select(
            "cte.start_author_id, cte.start_author_label, cte.start_author_image_url, "
            "cte.end_author_id, cte.end_author_label, cte.end_author_image_url, "
            "cte.depth, "
            "COALESCE(COUNT(pa1.publication_id), 0) AS author_total_pubs"
        )
        query_builder.join(
            "LEFT", PublicationAuthor, "pa1", "pa1.author_id = cte.start_author_id"
        )
        query_builder.join(
            "LEFT", PublicationAuthor, "pa2", "pa2.author_id = cte.end_author_id"
        )
        query_builder.and_condition("", "pa1.publication_id = pa2.publication_id", custom=True)
        query_builder.group_by(
            "cte.start_author_id, cte.start_author_label, cte.start_author_image_url, "
            "cte.end_author_id, cte.end_author_label, cte.end_author_image_url, cte.depth"
        )
        query_builder.order_by("cte.depth", ascending=True)
        query_builder.order_by("cte.start_author_id", ascending=True)
        query_builder.order_by("cte.end_author_id", ascending=True)

        query_builder.parameters = base_query_builder.parameters
        return query_builder


