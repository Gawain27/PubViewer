from com.gwngames.server.entity.base.Journal import Journal
from com.gwngames.server.query.QueryBuilder import QueryBuilder


class JournalQuery:
    @staticmethod
    def get_journals(session):
        query_builder = QueryBuilder(session, Journal.__tablename__, alias="j")
        query_builder.select("""
            j.id as "Journal ID",
            to_camel_case(j.title) as "Journal Title",
            'https://scimagojr.com/' || j.link as "Journal Page",
            j.year as "Year",
            j.sjr as "SJR",
            j.q_rank as "Journal Rank",
            j.h_index as "H Index",
            j.total_docs as "Total Documents",
            j.total_docs_3years as "Total Documents (3 Years)",
            j.total_refs as "Total References",
            j.total_cites_3years as "Total Citations (3 Years)",
            j.citable_docs_3years as "Citable Documents (3 Years)",
            j.cites_per_doc_2years as "Cites per Document (2 Years)",
            j.refs_per_doc as "References per Document",
            j.female_percent as "Female Percentage"
        """)
        query_builder.and_condition("", "j.q_rank IS NOT NULL", custom=True)
        query_builder.and_condition("", "j.title IS NOT NULL", custom=True)
        return query_builder

    @staticmethod
    def build_authors_from_journals_query(session, journal_ids: str):
        numbers = journal_ids.split(',')
        journal_ids = ','.join(f"({num})" for num in numbers)

        author_query = QueryBuilder(
            pool=session,
            table_name="author",
            alias="a"
        )
        (author_query.join(
            join_type="INNER",
            other="publication_author",
            join_alias="pa",
            this_field="id",
            other_field="author_id"
        ).join(
            join_type="INNER",
            other="publication",
            join_alias="p",
            on_condition="pa.publication_id = p.id"
        ).join(
            join_type="INNER",
            other="google_scholar_author",
            join_alias="gsa",
            on_condition="gsa.author_key = a.id"
        ).join(
            join_type="INNER",
            other=f"(VALUES {journal_ids})",
            join_alias="journ_id(id)",
            on_condition="p.journal_id = journ_id.id"

         #.and_condition(
          #  parameter="",
           # value=f"p.journal_id IN ({journal_ids})",
            #custom=True
        ).select("DISTINCT a.name, a.id"))

        return author_query

    @staticmethod
    def build_publications_from_journals_query(session, journal_ids: str):
        publication_query = QueryBuilder(
            pool=session,
            table_name="publication",
            alias="p"
        )

        publication_query.and_condition(
            parameter="",
            value=f"p.journal_id IN ({journal_ids})",
            custom=True
        )

        publication_query.select("DISTINCT p.id")

        return publication_query


