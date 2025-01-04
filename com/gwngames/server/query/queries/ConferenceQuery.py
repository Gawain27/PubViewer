from com.gwngames.server.entity.base.Conference import Conference
from com.gwngames.server.query.QueryBuilder import QueryBuilder


class ConferenceQuery:
    @staticmethod
    def get_conferences(session):
        query_builder = QueryBuilder(session, Conference.__tablename__, alias="c")
        query_builder.select("""
    c.id as "ID",
    c.title as "Title",
    c.acronym as "Acronym",
    c.publisher as "Publisher",
    c.rank,
    c.note,
    c.dblp_link as "Dblp Link",
    c.primary_for as "Primary For",
    c.comments,
    c.average_rating as "Average Rating"
""")
        query_builder.and_condition("", "c.rank IS NOT NULL", custom=True)
        return query_builder
