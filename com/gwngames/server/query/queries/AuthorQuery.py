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
            f"""
            a.id AS "Author ID",
            to_camel_case(a.name) AS "Name",
            CASE 
                WHEN a.role = '?' THEN a.organization 
                ELSE a.role || ' - ' || a.organization 
            END AS "Organization",
            a.image_url as "Image url",
            a.homepage_url as "Homepage",
            COALESCE(g.author_id, 'N/A') AS "Scholar ID",
            COALESCE(g.profile_url, 'N/A') AS "Scholar Profile",
            COALESCE(g.verified, 'N/A') AS "Verified on",
            COALESCE(g.h_index, 0) AS "H Index",
            COALESCE(g.i10_index, 0) AS "I10 Index",
            COALESCE(STRING_AGG(DISTINCT to_camel_case(i.name), ', '), 'N/A') AS "Interests",

            CASE 
                WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank)
                ELSE 'N/A'
            END AS "Frequent Conf. Rank",

            CASE 
                WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)
                ELSE 'N/A'
            END AS "Frequent Journal Rank",

            CASE 
                WHEN COUNT(j.sjr) > 0 THEN ROUND(
                    CAST(
                        AVG(
                            CAST(
                                COALESCE(
                                    NULLIF(REGEXP_REPLACE(j.sjr, '[^0-9.]', ''), ''), 
                                    '0'
                                ) AS FLOAT
                            )
                        ) * 10 AS NUMERIC
                    ), 2
                )
                ELSE 0
            END AS "Avg. SJR Score",
            (select sum(tot) from(
            select gsp.total_citations as tot from publication_author paa, publication pp, google_scholar_publication gsp
            where {author_id} = paa.author_id and gsp.publication_key = pp.id and pp.id = paa.publication_id
            group by pp.title, gsp.total_citations
            )) AS "Total Cites",
            COALESCE((select count(*) from publication pp, publication_author paa where paa.author_id = {author_id}
                and pp.id = paa.publication_id), 0) AS "Publications Found"
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
        # CTE 1: author_base
        author_base_qb = (
            QueryBuilder(pool=session, table_name="author", alias="a")
            .join("INNER", "google_scholar_author", "gsa", on_condition="gsa.author_key = a.id")
            .select("""
                a.id,
                a.name,
                a.role,
                a.organization,
                a.image_url
            """)
        )

        # CTE 2: interests
        interests_qb = (
            QueryBuilder(pool=session, table_name="author_interest", alias="ai")
            .join("INNER", "interest", "i", on_condition="i.id = ai.interest_id")
            .select("""
                ai.author_id,
                COALESCE(
                    STRING_AGG(DISTINCT to_camel_case(i.name), ', '),
                    'N/A'
                ) AS interests
            """)
            .group_by("ai.author_id")
        )

        # CTE 3: freq_conf_rank
        freq_conf_rank_qb = (
            QueryBuilder(pool=session, table_name="publication_author", alias="pa")
            .join("INNER", "publication", "p", on_condition="p.id = pa.publication_id")
            .join("INNER", "conference", "c", on_condition="c.id = p.conference_id")
            .select("""
                pa.author_id,
                CASE 
                    WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank)
                    ELSE '-'
                END AS freq_conf_rank
            """)
            .group_by("pa.author_id")
        )

        # CTE 4: freq_journal_rank
        freq_journal_rank_qb = (
            QueryBuilder(pool=session, table_name="publication_author", alias="pa")
            .join("INNER", "publication", "p", on_condition="p.id = pa.publication_id")
            .join("INNER", "journal", "j", on_condition="j.id = p.journal_id")
            .select("""
                pa.author_id,
                CASE 
                    WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)
                    ELSE '-'
                END AS freq_journal_rank
            """)
            .group_by("pa.author_id")
        )

        # CTE 5: avg_sjr_score
        avg_sjr_score_qb = (
            QueryBuilder(pool=session, table_name="publication_author", alias="pa")
            .join("INNER", "publication", "p", on_condition="p.id = pa.publication_id")
            .join("INNER", "journal", "j", on_condition="j.id = p.journal_id")
            .select("""
                pa.author_id,
                CASE 
                    WHEN COUNT(j.sjr) > 0 THEN 
                        ROUND(
                            CAST(
                                AVG(
                                    CAST(
                                        COALESCE(
                                            NULLIF(REGEXP_REPLACE(j.sjr, '[^0-9.]', ''), ''), 
                                            '0'
                                        ) AS FLOAT
                                    )
                                ) * 10 AS NUMERIC
                            ), 2
                        )
                    ELSE 0
                END AS avg_sjr_score
            """)
            .group_by("pa.author_id")
        )

        main_qb = QueryBuilder(pool=session, table_name="author_base", alias="ab")

        main_qb \
            .with_cte("author_base", author_base_qb) \
            .with_cte("interests", interests_qb) \
            .with_cte("freq_conf_rank", freq_conf_rank_qb) \
            .with_cte("freq_journal_rank", freq_journal_rank_qb) \
            .with_cte("avg_sjr_score", avg_sjr_score_qb)

        main_qb.select("""
            ab.id                  AS "Author ID",
            to_camel_case(ab.name) AS "Name",
            CASE
               WHEN ab.role = '?' THEN ab.organization
               ELSE ab.role || ' - ' || ab.organization
            END                    AS "Organization",
            ab.image_url           AS "Image url",
            i.interests            AS "Interests",
            CASE WHEN fc.freq_conf_rank IS NOT NULL THEN fc.freq_conf_rank ELSE 'N/A' END     AS "Frequent Conf. Rank",
            CASE WHEN fj.freq_journal_rank IS NOT NULL THEN fj.freq_journal_rank ELSE 'N/A' END   AS "Frequent Journal Rank",
            CASE WHEN asjr.avg_sjr_score IS NOT NULL THEN asjr.avg_sjr_score ELSE 0 END     AS "Avg. SJR Score"
        """)

        # Left joins to each of the other CTEs
        main_qb \
            .join("LEFT", "interests", "i", on_condition="ab.id = i.author_id") \
            .join("LEFT", "freq_conf_rank", "fc", on_condition="ab.id = fc.author_id") \
            .join("LEFT", "freq_journal_rank", "fj", on_condition="ab.id = fj.author_id") \
            .join("LEFT", "avg_sjr_score", "asjr", on_condition="ab.id = asjr.author_id")

        return main_qb

    @staticmethod
    def build_author_group_query_batch(session, author_ids):
        author_ids = ",".join(map(str, author_ids))
        numbers = author_ids.split(',')
        author_ids = ','.join(f"({num})" for num in numbers)

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
        #qb.and_condition("", f"start_author.id IN ({author_ids})", custom=True)
        qb.join("INNER", f"(VALUES {author_ids})", "id_author(id)", on_condition="start_author.id = id_author.id")
        qb.select(
            """
            start_author.id AS start_author_id,
            to_camel_case(start_author.name) AS start_author_label,
            start_author.image_url AS start_author_image_url,
            end_author.id AS end_author_id,
            to_camel_case(end_author.name) AS end_author_label,
            end_author.image_url AS end_author_image_url
            """
        )
        qb.group_by(
            "start_author.id", "start_author.name", "start_author.image_url",
            "end_author.id", "end_author.name", "end_author.image_url"
        )
        return qb

    @staticmethod
    def build_authors_from_pub_query(session, pub_ids):
        publication_author_query = QueryBuilder(
            pool=session,
            table_name="publication_author",
            alias="pa"
        )

        publication_author_query.and_condition(
            parameter="pa.publication_id",
            value=f"pa.publication_id IN ({','.join([val.strip() for val in pub_ids.split(',') if val.strip()])})",
            custom=True
        )

        author_query = QueryBuilder(
            pool=session,
            table_name="author",
            alias="a"
        )

        author_query.join(
            join_type="INNER",
            other="publication_author",  # Use the actual table name
            join_alias="pa",
            this_field="id",  # Author.id
            other_field="author_id"  # PublicationAuthor.author_id
        )

        author_query.select("DISTINCT a.name")

        author_query.and_condition(
            parameter="pa.publication_id",
            value=f"pa.publication_id IN ({pub_ids})",
            custom=True
        )

        return author_query

    @staticmethod
    def build_co_authors_query(session, author_id):
        co_author_query1 = QueryBuilder(session, "author_coauthor", "aco")
        co_author_query1.and_condition("",f"aco.author_id = {author_id}", custom=True)
        co_author_query1.select("aco.coauthor_id as id")

        co_author_query2 = QueryBuilder(session, "author_coauthor", "aco")
        co_author_query2.and_condition("", f"aco.coauthor_id = {author_id}", custom=True)
        co_author_query2.select("aco.author_id as id")

        coauthors = QueryBuilder(session, f"({co_author_query1.build_query_string()} UNION {co_author_query2.build_query_string()})", "acoa")
        coauthors.select("DISTINCT acoa.id")
        return coauthors


