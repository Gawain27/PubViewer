import asyncio
import copy
import logging
import math
import os
import traceback
from collections import defaultdict

from quart import Quart, render_template, jsonify, request

# psycopg3 async usage
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

from com.gwngames.client.general.GeneralDetailOverview import GeneralDetailOverview
from com.gwngames.client.general.GeneralTableCache import get_query_builder
from com.gwngames.client.general.GeneralTableOverview import GeneralTableOverview
from com.gwngames.config.Context import Context
from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.entity.variant.scholar.GoogleScholarAuthor import GoogleScholarAuthor
from com.gwngames.server.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication
from com.gwngames.server.query.QueryBuilder import QueryBuilder
from com.gwngames.server.query.queries.AuthorQuery import AuthorQuery
from com.gwngames.server.query.queries.ConferenceQuery import ConferenceQuery
from com.gwngames.server.query.queries.JournalQuery import JournalQuery
from com.gwngames.server.query.queries.PublicationQuery import PublicationQuery
from com.gwngames.utils.JsonReader import JsonReader


class ExcludeFilter(logging.Filter):
    def filter(self, record):
        return not any(
            record.name.startswith(mod) for mod in ('httpx', 'httpcore', 'urllib3', 'selenium')
        )


# ----------------- REGION MAIN ---------

# Initialize the logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize context
ctx: Context = Context()
ctx.set_current_dir(os.getcwd())

# Load config
conf_reader = JsonReader(JsonReader.CONFIG_FILE_NAME)
ctx.set_config(conf_reader)

# Initialize Quart
app = Quart(__name__)
app.logger.setLevel(logging.DEBUG)

pool: AsyncConnectionPool

# --------------- REGION STARTUP --------------------

@app.before_serving
async def setup_pool():
    """
    Initialize the async connection pool before the first request.
    """
    global pool
    try:
        config: JsonReader = ctx.get_config()

        max_active_transactions = config.get_value("max_active_transactions")
        max_pool_transactions = config.get_value("max_pool_transactions")

        db_url = config.get_value("db_url")
        db_name = config.get_value("db_name")
        db_user = config.get_value("db_user")
        db_password = config.get_value("db_password")
        db_port = config.get_value("db_port")
        database_url = f"postgresql://{db_user}:{db_password}@{db_url}:{db_port}/{db_name}"

        # Use `await pool.open()` or a context manager
        pool = AsyncConnectionPool(
            conninfo=database_url,
            min_size=1,
            max_size=max_pool_transactions,
            num_workers=max_active_transactions,
            kwargs={"autocommit": True, "row_factory": dict_row}
        )
        await pool.open()
        ctx.set_pool(pool)
        logger.info("Async connection pool created successfully.")
    except Exception as e:
        logger.error(f"Failed to create async connection pool: {e}")
        raise


@app.after_serving
async def close_pool():
    """
    Close the async connection pool after the server stops.
    """
    try:
        if pool:
            await pool.close()
            logger.info("Async connection pool closed successfully.")
    except Exception as e:
        logger.error(f"Error while closing the async connection pool: {e}")


@app.get('/')
async def start_client():
    """
    Render the home page with dynamic content.
    """
    try:
        global pool
        author_count_query = QueryBuilder(pool, GoogleScholarAuthor.__tablename__, 'g').select('COUNT(*)')
        publication_count_query = QueryBuilder(pool, GoogleScholarPublication.__tablename__, 'g').select('COUNT(*)')

        # Execute the queries and fetch results
        author_count_result = await author_count_query.execute()
        publication_count_result = await publication_count_query.execute()

        # Extract counts
        author_count = list(author_count_result)[0]["count"]
        publication_count = list(publication_count_result)[0]["count"]

        # Render the home page with these statistics
        return await render_template(
            'template.html',
            content=(
                f'<p>Discover <strong>{author_count}</strong> authors and their '
                f'<strong>{publication_count}</strong> groundbreaking publications.</p>'
            ),
            current_year=2024
        )
    except Exception as e:
        app.logger.error(f"Error in start_client: {e}")
        return await render_template(
            'template.html',
            content='<p>Error loading home page. Please try again later.</p>',
            current_year=2024
        )

# -------------------- REGION APPLICATION -------------------------------

@app.get('/about')
async def about():
    return await render_template(
        'template.html',
        content='<p>TODO.</p>',
        current_year=2025
    )


@app.get('/publications')
async def publications():
    """
    Example: This route continues to rely on the same logic,
    but internally you must ensure your QueryBuilder calls use async psycopg,
    not synchronous SQLAlchemy.
    """
    author = request.args.get('value', None)

    query_builder: QueryBuilder = PublicationQuery.build_overview_publication_query(ctx.get_pool())
    table_component = GeneralTableOverview(query_builder, "Publications Overview", limit=ctx.get_config().get_value("max_overview_rows"))

    if author is not None:
        author = f"%{author}%"
        query_builder.having_and("STRING_AGG(DISTINCT lower(a.name), ', ')", author,
                                 operator="LIKE", is_case_sensitive=False)
        query_builder.offset(0).limit(ctx.get_config().get_value("max_overview_rows"))

        # In an async world, you might do:
        external_records = await query_builder.execute()
        table_component.external_records = external_records

    table_component.entity_class = query_builder.table_name
    table_component.alias = query_builder.alias
    table_component.add_filter("Pub. ID", "string", "Pub. ID (, OR)", or_split=True, equal=True)
    table_component.add_filter("Title", "string", "Title (, OR)", or_split=True)
    table_component.add_filter(
        "CASE WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank) ELSE '-' END",
        "string", "Conf. Rank (, OR)", is_aggregated=True, or_split=True, equal=True
    )
    table_component.add_filter(
        "CASE WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank) ELSE '-' END",
        "string", "Journal Rank (, OR)", is_aggregated=True, or_split=True
    )
    table_component.add_filter("publication_year", "integer", "Year")
    table_component.add_filter(
        "STRING_AGG(DISTINCT lower(a.name), ', ')",
        "string",
        "Author (, AND)",
        is_aggregated=True,
        or_split=False
    )

    table_component.add_row_method("View Publication Details", "publication_details")

    return await render_template(
        "template.html",
        content=await table_component.render()
    )


@app.get('/publication_details')
async def publication_details():
    row_name = request.args.get('id')
    if row_name is None:
        row_name = request.args.get('value')

    query_builder = PublicationQuery.build_specific_publication_query(ctx.get_pool(), row_name)

    data_viewer = GeneralDetailOverview(
        query_builder,
        title_field="Title",
        description_field="Description"
    )

    data_viewer.add_row_method("View Conference", "conferences", column_name="Conference")
    data_viewer.add_row_method("View Journal", "journals", column_name="Journal")

    return await render_template(
        'template.html',
        content=await data_viewer.render()
    )


@app.get('/researchers')
async def researchers():
    query_builder: QueryBuilder = AuthorQuery.build_author_overview_query(ctx.get_pool())

    table_component = GeneralTableOverview(query_builder, "Researchers Overview",
                                           limit=ctx.get_config().get_value("max_overview_rows"),
                                           image_field="Image url",
                                           enable_checkboxes=True
                                           )
    table_component.alias = query_builder.alias
    table_component.entity_class = query_builder.table_name
    table_component.add_filter("Author ID", filter_type="string", label="Author ID (, OR)", or_split=True, equal=True)
    table_component.add_filter("Name", filter_type="string", label="Name (, OR)", or_split=True)
    table_component.add_filter(
        "COALESCE(STRING_AGG(DISTINCT i.name, ', '), 'N/A')",
        filter_type="string", label="Interest (, AND)", is_aggregated=True, or_split=False
    )
    table_component.add_filter(
        "CASE WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank) ELSE '-' END",
        filter_type="string", label="Avg. Conf. Rank (, OR)", is_aggregated=True, or_split=True, equal=True
    )
    table_component.add_filter(
        "CASE WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank) ELSE '-' END",
        filter_type="string", label="Avg. Jour. Rank (, OR)", is_aggregated=True, or_split=True
    )
    table_component.add_row_method("View Author Details", "researcher_detail")
    table_component.add_page_method("View Combined Network", "author_network")

    return await render_template(
        "template.html",
        content=await table_component.render()
    )


@app.get('/researcher_detail')
async def researcher_detail():
    row_name = request.args.get('Author ID')
    if row_name is None:
        row_name = request.args.get('value')
    query_builder = AuthorQuery.build_author_query_with_filter(ctx.get_pool(), author_id=int(row_name))

    data_viewer = GeneralDetailOverview(
        query_builder,
        title_field="Name",
        description_field="Homepage",
        image_field="Image url"
    )

    data_viewer.add_row_method("View Publications", "publications", "Name")
    data_viewer.add_row_method("View Network", "author_network", "Author ID")

    return await render_template(
        'template.html',
        content=await data_viewer.render()
    )


@app.get('/conferences')
async def conferences():
    acronym = request.args.get('value', None)

    query_builder: QueryBuilder = ConferenceQuery.get_conferences(ctx.get_pool())
    table_component = GeneralTableOverview(query_builder, "Conferences Overview", limit=ctx.get_config().get_value("max_overview_rows"))

    if acronym is not None:
        author = f"%{acronym}%"
        query_builder.and_condition("Acronym", author, operator="LIKE", is_case_sensitive=False)
        query_builder.offset(0).limit(100)
        table_component.external_records = await query_builder.execute()

    table_component.entity_class = query_builder.table_name
    table_component.alias = query_builder.alias
    table_component.add_filter("ID", filter_type="string", label="ID (, OR)", or_split=True, equal=True)
    table_component.add_filter("Title", "string", "Title (, OR)", or_split=True)
    table_component.add_filter("Acronym", "string", "Acronym (, OR)", or_split=True)
    table_component.add_filter("Rank", "string", "Rank (, OR)", or_split=True, equal=True)
    table_component.add_filter("Publisher", "string", "Publisher")

    return await render_template(
        "template.html",
        content=await table_component.render()
    )


@app.get('/journals')
async def journals():
    query_builder: QueryBuilder = JournalQuery.get_journals(ctx.get_pool())

    table_component = GeneralTableOverview(query_builder, "Journals Overview", limit=ctx.get_config().get_value("max_overview_rows"))
    table_component.entity_class = query_builder.table_name
    table_component.alias = query_builder.alias
    table_component.add_filter("ID", filter_type="string", label="ID (, OR)", or_split=True, equal=True)
    table_component.add_filter("title", "string", "Title (, OR)", or_split=True)
    table_component.add_filter("q_rank", "string", "Rank (, OR)", or_split=True, equal=True)
    table_component.add_filter("Year", "integer", "Year")

    return await render_template(
        "template.html",
        content=await table_component.render()
    )


@app.get('/author_network')
async def author_network():
    """
    Renders the author network page, starting from a given author ID.
    """
    start_author_ids = request.args.get('value', None)

    if not start_author_ids:
        return "Error: Author ID is required", 400

    try:
        author_data: QueryBuilder = QueryBuilder(ctx.get_pool(), Author.__tablename__, 'a')
        author_data.select("a.name")
        author_data.and_condition("", f"a.id IN ({start_author_ids})", custom=True)
        results = await author_data.execute()

        if not results:
            return "Error: Author(s) not found", 404

        start_author_labels = ",".join([r["name"] for r in results])
        max_depth = int(conf_reader.get_value("max_generative_depth"))

        return await render_template(
            "template.html",
            title="Author Network",
            content=await render_template(
                "graph_component.html",
                start_id=start_author_ids,
                start_label=start_author_labels,
                max_depth=max_depth
            ),
            popup=await render_template("popup.html")
        )
    except IndexError as e:
        logging.error(f"Error in author_network: {str(e)}")
        return "Error: Author(s) not found", 404
    except Exception as e:
        logging.error(f"Error in author_network: {str(e)}")
        return "Internal Server Error", 500


# --------------- REGION API CALLS --------------------

@app.post("/fetch-author-detail")
async def fetch_author_detail():
    data = await request.get_json()
    author_id = data.get("author_id", None)
    if not author_id:
        return jsonify({"error": "Error: Author ID is required"})

    qb = AuthorQuery.build_author_query_with_filter(pool, int(author_id))
    author = await qb.execute()
    author = author[0] if author else None
    if not author:
        return jsonify({"error": "Error: Author ID is not found"})

    app.logger.info("Fetched author detail: %s", author)

    org = f"{author["Role"]} - {author["Organization"]}" if author["Role"] != "?" else author["Organization"]
    return jsonify( {"author_data":
        {
            "Organization": f"{org}",
            "hIndex": author["H Index"],
            "i10Index": author["I10 Index"],
            "citesTotal": author["Total Cites"],
            "pubTotal": author["Publications Found"],
            "avg_conference_rank": author["Avg. Conf. Rank"],
            "avg_journal_rank": author["Avg. Journal Rank"]
        }}
    )

@app.post("/fetch_data")
async def fetch_data():
    """
    We retrieve the same QueryBuilder from the table_id cookie,
    reapply offset, limit, filters, then return rows in JSON.
    """
    table_id = request.args.get("table_id")
    if not table_id:
        return jsonify({"error": "No table_id found"}), 400

    qb = get_query_builder(table_id)
    if not qb:
        return jsonify({"error": "Invalid or expired table_id"}), 404

    form = await request.form
    offset = int(form.get("offset", 0))
    limit = int(form.get("limit", 100))

    qb.offset(offset).limit(limit)

    rows = await qb.execute()

    # Count total rows
    count_query = qb.clone(no_limit=True, no_offset=True)
    count_query.select(f"COUNT(*) AS count")
    count_query.order_by_fields = []
    count_data = await count_query.execute()
    total_count = sum(row["count"] for row in count_data)

    return jsonify({
        "rows": rows,
        "offset": offset,
        "limit": limit,
        "total_count": total_count
    })


@app.post("/generate-graph")
async def generate_graph():
    try:
        data = await request.get_json()
        start_author_id = int(data["start_author_id"])
        max_depth = int(data["depth"])
        max_tuple_per_query = int(conf_reader.get_value("max_tuple_per_query"))

        # BFS variables
        start_depth = 0
        authors_seen = set()
        authors_to_query = [start_author_id]
        edges = []
        nodes = {}

        # 0 - Starting author info (in case of no results)

        sql_author = await QueryBuilder(ctx.get_pool(), Author.__tablename__, 'a').select(
            'a.name, a.image_url').and_condition('a.id', start_author_id).execute()
        starting_author = sql_author[0]

        # ---------------------------
        # 1) BFS expansion in N sub-batches
        # ---------------------------
        while start_depth < max_depth:
            current_authors = list(set(authors_to_query) - authors_seen)
            authors_to_query.clear()
            if not current_authors:
                break

            authors_seen.update(current_authors)

            # Divide authors into up to 8 chunks, fetch in parallel
            results_this_depth = []
            chunk_size = math.ceil(len(current_authors) / max_tuple_per_query)
            tasks = []
            for i in range(0, len(current_authors), chunk_size):
                chunk = current_authors[i : i + chunk_size]
                tasks.append(asyncio.create_task(fetch_author_links_batch(chunk)))
            sub_results_list = await asyncio.gather(*tasks)

            # Combine sub-results
            for sub_results in sub_results_list:
                results_this_depth.extend(sub_results)

            # Process BFS expansions
            for row in results_this_depth:
                s_id = row["start_author_id"]
                e_id = row["end_author_id"]
                edges.append(
                    (
                        s_id,
                        row["start_author_label"],
                        row["start_author_image_url"],
                        e_id,
                        row["end_author_label"],
                        row["end_author_image_url"]
                    )
                )
                if e_id not in authors_seen:
                    authors_to_query.append(e_id)

            start_depth += 1

        # ---------------------------
        # 2) Minimal node info
        # ---------------------------
        # Ensure the starting node is in the graph
        nodes[start_author_id] = {
            "id": start_author_id,
            "label": starting_author["name"],
            "image": starting_author["image_url"],
        }

        # Build edges, if any
        for (s_id, s_label, s_img, e_id, e_label, e_img) in edges:
            if s_id not in nodes:
                nodes[s_id] = {"id": s_id, "label": s_label, "image": s_img or ""}
            if e_id not in nodes:
                nodes[e_id] = {"id": e_id, "label": e_label, "image": e_img or ""}

        # ---------------------------
        # 3) Fetch publication info (ranks, years) in N cores sub-batches
        # ---------------------------
        unique_pairs = set()
        for (s_id, _, _, e_id, _, _) in edges:
            pair = tuple(sorted((s_id, e_id)))
            unique_pairs.add(pair)

        pair_to_ranks_freq = {}
        pair_to_years_freq = {}
        pairs_list = list(unique_pairs)

        if pairs_list:
            chunk_size = math.ceil(len(pairs_list) / max_tuple_per_query)
            tasks = []
            for i in range(0, len(pairs_list), chunk_size):
                sub_batch = pairs_list[i : i + chunk_size]
                tasks.append(asyncio.create_task(fetch_pub_info_subbatch(sub_batch)))
            results = await asyncio.gather(*tasks)

            # Merge partial dictionaries
            for (ranks_dict, years_dict) in results:
                for p, freq_map in ranks_dict.items():
                    pair_to_ranks_freq[p] = freq_map
                for p, y_map in years_dict.items():
                    pair_to_years_freq[p] = y_map

        # ---------------------------
        # 4) Build links with publication data
        # ---------------------------
        links = []
        for (s_id, s_label, s_img, e_id, e_label, e_img) in edges:
            pair_key = tuple(sorted((s_id, e_id)))
            rank_counts = pair_to_ranks_freq.get(pair_key, {})
            years_map = pair_to_years_freq.get(pair_key, {})

            conf_freq = {}
            jour_freq = {}
            unranked = 0
            for rank_str, cnt in rank_counts.items():
                if rank_str in ["A*", "A", "B", "C"]:
                    conf_freq[rank_str] = conf_freq.get(rank_str, 0) + cnt
                elif rank_str in ["Q1", "Q2", "Q3", "Q4"]:
                    jour_freq[rank_str] = jour_freq.get(rank_str, 0) + cnt
                else:
                    unranked += cnt

            best_conf = max(conf_freq, key=conf_freq.get) if conf_freq else "Unranked"
            best_jour = max(jour_freq, key=jour_freq.get) if jour_freq else "Unranked"

            link_obj = {
                "source": s_id,
                "target": e_id,
                "avg_conf_rank": best_conf,
                "avg_journal_rank": best_jour,
                "Unranked": unranked,
            }
            for yr, val in years_map.items():
                link_obj[str(yr)] = val
            for rank, val in conf_freq.items():
                link_obj[str(rank)] = val
            for rank, val in jour_freq.items():
                link_obj[str(rank)] = val

            links.append(link_obj)

        return jsonify({"nodes": list(nodes.values()), "links": links})

    except Exception as e:
        app.logger.error(f"Error: {e}")
        app.logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


# ---------------------------
# ASYNC HELPER FUNCTIONS
# ---------------------------
async def fetch_author_links_batch(author_ids):
    if not author_ids:
        return []
    try:
        return await AuthorQuery.build_author_group_query_batch(pool, author_ids).execute()
    except Exception as e:
        app.logger.error(f"fetch_author_links_batch error: {e}")
        return []

async def fetch_pub_info_subbatch(pairs):
    if not pairs:
        return {}, {}
    ranks_rows = await fetch_pub_ranks_batch(pairs)
    years_rows = await fetch_pub_years_batch(pairs)

    ranks_freq = defaultdict(lambda: defaultdict(int))
    years_freq = defaultdict(lambda: defaultdict(int))

    for row in ranks_rows:
        a1 = min(row["aid1"], row["aid2"])
        a2 = max(row["aid1"], row["aid2"])
        r = row["rank_name"]
        ranks_freq[(a1, a2)][r] += row["rank_total_pubs"]

    for row in years_rows:
        a1 = min(row["aid1"], row["aid2"])
        a2 = max(row["aid1"], row["aid2"])
        y = row["publication_year"]
        years_freq[(a1, a2)][y] += row["publication_count"]

    return ranks_freq, years_freq

async def fetch_pub_ranks_batch(pairs):
    if not pairs:
        return []
    try:
        return await PublicationQuery.build_author_publication_query_batch(pool, pairs).execute()
    except Exception as e:
        app.logger.error(f"fetch_pub_ranks_batch error: {e}")
        return []

async def fetch_pub_years_batch(pairs):
    if not pairs:
        return []
    try:
        return await PublicationQuery.build_author_publication_year_query_batch(pool, pairs).execute()
    except Exception as e:
        app.logger.error(f"fetch_pub_years_batch error: {e}")
        return []

async def fetch_author_links(author_id: int) -> list[dict]:
    """
    Example async function to fetch all co-author links for a given author
    using psycopg3.
    This replaces synchronous calls like "query.execute()" with an async approach.
    """
    try:
        rows = await AuthorQuery.build_author_group_query(pool, author_id).execute()
        return rows
    except Exception as ex:
        app.logger.error(f"fetch_author_links error for author_id={author_id}: {ex}")
        return []


async def enrich_link_with_publications(
        start_id,
        start_label,
        start_image,
        end_id,
        end_label,
        end_image,
        avg_conf_rank,
        avg_journal_rank
) -> dict:
    """
    Fetch extra info about publications for the link between start_id and end_id
    in parallel.
    """
    if avg_journal_rank not in ["Q1", "Q2", "Q3", "Q4", None]:
        avg_journal_rank = "Unranked"

    if avg_conf_rank not in ["A*", "A", "B", "C", None]:
        avg_conf_rank = "Unranked"

    link = {
        "source": start_id,
        "target": end_id,
        "avg_conf_rank": avg_conf_rank,
        "avg_journal_rank": avg_journal_rank
    }

    # We’ll run two queries in parallel:
    # 1) Count publications by rank
    # 2) Count publications by year
    pub_ranks_task = asyncio.create_task(fetch_pub_ranks(start_id, end_id))
    pub_years_task = asyncio.create_task(fetch_pub_years(start_id, end_id))
    pub_ranks_result, pub_years_result = await asyncio.gather(pub_ranks_task, pub_years_task)

    # Attach rank counts
    link["Unranked"] = 0
    for rank_assoc in pub_ranks_result:
        if str(rank_assoc["rank_name"]) not in ['Q1','Q2','Q3','Q4','A*','A','B','C']:
            link["Unranked"] +=  rank_assoc["rank_total_pubs"]
        else:
            link[str(rank_assoc["rank_name"])] = rank_assoc["rank_total_pubs"]

    # Attach year counts
    for year_assoc in pub_years_result:
        link[str(year_assoc["publication_year"])] = year_assoc["publication_count"]

    return {
        "start_label": start_label,
        "start_image": start_image,
        "end_label": end_label,
        "end_image": end_image,
        "link": link
    }


async def fetch_pub_ranks(aid1: int, aid2: int) -> list[dict]:
    """
    Example of an async query that returns publication rank info for two authors.
    """
    try:
        rows = await PublicationQuery.build_author_publication_query(pool, aid1, aid2).execute()
        return rows
    except Exception as ex:
        app.logger.error(f"fetch_pub_ranks error for {aid1}-{aid2}: {ex}")
        return []


async def fetch_pub_years(aid1: int, aid2: int) -> list[dict]:
    """
    Example of an async query that returns publication-year info for two authors.
    """
    try:
        rows = await PublicationQuery.build_author_publication_year_query(pool, aid1, aid2).execute()
        return rows
    except Exception as ex:
        app.logger.error(f"fetch_pub_years error for {aid1}-{aid2}: {ex}")
        return []


if __name__ == '__main__':
    # For dev/test:
    # Note that Quart’s debug reloader can cause multiple event loops.
    # In production, run with a proper ASGI server (e.g., hypercorn or uvicorn).
    # I use hypercorn because EZ, but there is really no valid distinction
    app.run(host="0.0.0.0", port=5000, debug=True)
