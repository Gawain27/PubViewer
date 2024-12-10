from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.entity.base.Conference import Conference
from com.gwngames.server.entity.base.Journal import Journal
from com.gwngames.server.entity.base.Publication import Publication
from com.gwngames.server.entity.base.Relationships import PublicationAuthor
from com.gwngames.server.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication
from com.gwngames.server.query.QueryBuilder import QueryBuilder


class PublicationQuery:
    @staticmethod
    def build_filtered_publication_query(session, title: str):
        """
        Build a query to filter publications by title, including related journal and conference data.
        """
        # Base QueryBuilder for Publication
        publication_query = QueryBuilder(session, Publication, "p")
        google_scholar_query = QueryBuilder(session, GoogleScholarPublication, "g")
        journal_query = QueryBuilder(session, Journal, "j")
        conference_query = QueryBuilder(session, Conference, "c")

        # Joins for relations
        publication_query.join(
            "LEFT", google_scholar_query, "g", on_condition="p.id = g.id"
        ).join(
            "LEFT", journal_query, "j", on_condition="j.id = p.journal_id"
        ).join(
            "LEFT", conference_query, "c", on_condition="c.id = p.conference_id"
        )

        # Add WHERE condition for title
        publication_query.and_condition("p.title", title)

        # Select required fields and aggregations
        publication_query.select(
            """
            p.id AS publication_id as Publication ID,
            p.title AS Title,
            p.url as URL,
            p.publication_year as Publication Year,
            p.pages as Pages,
            p.publisher as Publisher,
            p.description as Description,
            COALESCE(g.publication_id, 'N/A') AS Scholar Pub. ID,
            COALESCE(g.title_link, 'N/A') AS Resource Link,
            COALESCE(g.pdf_link, 'N/A') AS PDF Link,
            COALESCE(g.total_citations, 0) AS Total Citations,
            COALESCE(g.related_articles_url, 'N/A') AS Related Article URL,
            COALESCE(g.all_versions_url, 'N/A') AS Versions URL,
            CASE 
                WHEN COUNT(j.id) > 0 THEN MODE() WITHIN GROUP (ORDER BY REGEXP_REPLACE(j.sjr, '[^0-9.]', ''))
                ELSE 'N/A'
            END AS Journal Score,
            CASE 
                WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)
                ELSE 'N/A'
            END AS SJR Journal Rank,
            CASE 
                WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank)
                ELSE 'N/A'
            END AS Conference Rank
            """
        )

        # Grouping
        publication_query.group_by(
            "p.id", "p.title", "p.url", "p.publication_year",
            "p.pages", "p.publisher", "p.description",
            "g.publication_id", "g.title_link", "g.pdf_link", "g.total_citations",
            "g.related_articles_url", "g.all_versions_url", "g.cites_id"
        )

        # Limit the result to 1
        publication_query.limit(1)

        return publication_query

    @staticmethod
    def build_overview_publication_query(session):
        """
        Build an overview query for publications, including associated journal, conference, and Google Scholar data.
        """
        # Base QueryBuilder for Publication
        publication_query = QueryBuilder(session, Publication, "p")
        journal_query = QueryBuilder(session, Journal, "j")
        conference_query = QueryBuilder(session, Conference, "c")
        google_scholar_query = QueryBuilder(session, GoogleScholarPublication, "gsp")
        assoc_query = QueryBuilder(session, PublicationAuthor, "ass")
        author_query = QueryBuilder(session, Author, "a")


        # Joins for relations
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

        # Filter the query to include only records meeting the conditions
        publication_query.and_condition("",
            """
            (c.rank IS NOT NULL OR j.q_rank IS NOT NULL OR gsp.id IS NOT NULL)
            """, custom=True
        )

        # Select required fields and aggregations
        publication_query.select(
            """
            p.id AS publication_id,
            p.title AS publication_title,
            p.publication_year,
            p.publisher,
            STRING_AGG(DISTINCT a.name, ', ') AS authors,
            CASE 
                WHEN COUNT(j.id) > 0 THEN MODE() WITHIN GROUP (ORDER BY REGEXP_REPLACE(j.sjr, '[^0-9.]', ''))
                ELSE '0'
            END AS journal_sjr,
            CASE 
                WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)
                ELSE '-'
            END AS journal_qrank,
            CASE 
                WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank)
                ELSE '-'
            END AS conference_rank
            """
        )

        # Grouping
        publication_query.group_by(
            "p.id", "p.title", "p.publication_year", "p.publisher"
        )

        return publication_query





