from com.gwngames.server.entity.base.Conference import Conference
from com.gwngames.server.query.QueryBuilder import QueryBuilder


class ConferenceQuery:
    @staticmethod
    def get_conferences(session):
        query_builder = QueryBuilder(session, Conference.__tablename__, alias="c")
        query_builder.select("""
    c.id as "Conf ID",
    c.title as "Conference Title",
    c.acronym as "Acronym",
    c.publisher as "Publisher",
    c.rank as "Conference Rank",
    c.note as "Note",
    c.dblp_link as "Dblp Link",
    c.primary_for as "Primary For",
    c.average_rating as "Average Rating"
""")
        query_builder.and_condition("", "c.rank IS NOT NULL", custom=True)
        query_builder.and_condition("", "c.acronym IS NOT NULL", custom=True)
        return query_builder

    @staticmethod
    def build_authors_from_conferences_query(session, conference_ids: str):
        numbers = conference_ids.split(',')
        conference_ids = ','.join(f"({num})" for num in numbers)

        author_query = QueryBuilder(
            pool=session,
            table_name="author",
            alias="a"
        )
        author_query.join(
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
            other=f"(VALUES {conference_ids})",
            join_alias="conf_id(id)",
            on_condition="p.conference_id = conf_id.id"
        #.and_condition(
          #  parameter="",
           # value=f"p.conference_id IN ({conference_ids})",
            #custom=True
        ).select("DISTINCT a.name, a.id")

        return author_query

    @staticmethod
    def build_publications_from_conferences_query(session, conference_ids: str):
        publication_query = QueryBuilder(
            pool=session,
            table_name="publication",
            alias="p"
        )

        publication_query.and_condition(
            parameter="",
            value=f"p.conference_id IN ({conference_ids})",
            custom=True
        )

        publication_query.select("DISTINCT p.id")

        return publication_query

