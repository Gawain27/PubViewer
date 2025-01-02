from com.gwngames.server.entity.base.Journal import Journal
from com.gwngames.server.query.QueryBuilder import QueryBuilder


class JournalQuery:
    @staticmethod
    def getJournals(session):
        query_builder = QueryBuilder(session, Journal.__tablename__, alias="j")
        query_builder.select("""
            j.id as ID,
            j.title as "Title",
            j.link as "Link",
            j.year as "Year",
            j.sjr as "SJR",
            j.q_rank as "Rank",
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
        return query_builder
