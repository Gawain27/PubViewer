import asyncio
import copy
import logging
import os
import traceback

from quart import Quart, render_template, jsonify, request

# psycopg3 async usage
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

from com.gwngames.client.general.GeneralDetailOverview import GeneralDetailOverview
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

        pool = AsyncConnectionPool(
            conninfo=database_url,
            min_size=1,
            max_size=max_pool_transactions,
            num_workers=max_active_transactions,
            kwargs={"autocommit": True, "row_factory": dict_row}
        )
        ctx.set_pool(pool)
        logger.info("Async connection pool created successfully.")
    except Exception as e:
        logger.error(f"Failed to create async connection pool: {e}")
        raise


@app.after_serving
async def close_pool():
    """
    Close the async connection pool when the app stops.
    """
    global pool
    if pool:
        await pool.close()
        logger.info("Async connection pool closed successfully.")


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
    table_component.add_filter("Title", "string", "Title", is_case_sensitive=False)
    table_component.add_filter(
        "CASE WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank) ELSE '-' END",
        "string", "Conf. Rank", is_aggregated=True, is_case_sensitive=True
    )
    table_component.add_filter(
        "CASE WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank) ELSE '-' END",
        "string", "Journal Rank", is_aggregated=True, is_case_sensitive=False
    )
    table_component.add_filter("publication_year", "integer", "Year")
    table_component.add_filter(
        "STRING_AGG(DISTINCT lower(a.name), ', ')",
        "string",
        "Author",
        is_aggregated=True,
        is_case_sensitive=True
    )

    table_component.add_row_method("View Publication Details", "publication_details")

    return await render_template(
        "template.html",
        content=await table_component.render()
    )


@app.get('/publication_details')
async def publication_details():
    row_name = request.args.get('id')

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

    table_component = GeneralTableOverview(query_builder, "Researchers Overview", limit=ctx.get_config().get_value("max_overview_rows"), image_field="Image url")
    table_component.alias = query_builder.alias
    table_component.entity_class = query_builder.table_name
    table_component.add_filter("Name", filter_type="string", label="Name", is_case_sensitive=False)
    table_component.add_filter(
        "COALESCE(STRING_AGG(DISTINCT i.name, ', '), 'N/A')",
        filter_type="string", label="Interest", is_aggregated=True, is_case_sensitive=False
    )
    table_component.add_filter(
        "CASE WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank) ELSE '-' END",
        filter_type="string", label="Avg. Conf. Rank", is_aggregated=True, is_case_sensitive=True
    )
    table_component.add_filter(
        "CASE WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank) ELSE '-' END",
        filter_type="string", label="Avg. Q. Rank", is_aggregated=True, is_case_sensitive=False
    )
    table_component.add_row_method("View Author Details", "researcher_detail")

    return await render_template(
        "template.html",
        content=await table_component.render()
    )


@app.get('/researcher_detail')
async def researcher_detail():
    row_name = request.args.get('Author ID')

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
    table_component.add_filter("Title", "string", "Title", is_case_sensitive=False)
    table_component.add_filter("Acronym", "string", "Acronym", is_case_sensitive=False)
    table_component.add_filter("Rank", "string", "Rank", is_case_sensitive=True)
    table_component.add_filter("Publisher", "string", "Publisher", is_case_sensitive=False)

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
    table_component.add_filter("title", "string", "Title", is_case_sensitive=False)
    table_component.add_filter("q_rank", "string", "Rank", is_case_sensitive=True)
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
            )
        )
    except IndexError as e:
        logging.error(f"Error in author_network: {str(e)}")
        return "Error: Author(s) not found", 404
    except Exception as e:
        logging.error(f"Error in author_network: {str(e)}")
        return "Internal Server Error", 500


@app.post("/generate-graph")
async def generate_graph():
    """
    Generates a tree-structured author graph starting from a given author ID.
    This version uses async queries with psycopg3 and parallelizes queries
    both at each depth level and for each record in the for-loop.
    """
    try:
        data = await request.get_json()
        app.logger.debug("Received JSON payload: %s", data)

        start_author_id = data.get("start_author_id")
        if not start_author_id:
            return jsonify({"error": "start_author_id is required"}), 400

        max_depth = data.get("depth")
        if not max_depth:
            return jsonify({"error": "depth is required"}), 400

        # Convert inputs to integers
        start_author_id = int(start_author_id)
        max_depth = int(max_depth)

        start_depth = 0
        authors_seen = set()
        authors_to_query = [start_author_id]

        results = []

        # We'll store tasks by depth. At each depth, we query in parallel.
        # Starting author info (for node info)
        # In an async world, we can do a single SELECT
        # Also, since users can generate various networks, caching with this granularity is best
        sql_author = await QueryBuilder(ctx.get_pool(), Author.__tablename__, 'a').select(
            'a.name, a.image_url').and_condition('a.id', start_author_id).execute()
        starting_author = sql_author[0]

        # Depth-based BFS-like expansions
        while start_depth < max_depth:
            app.logger.info(f"Depth {start_depth} - authors to query: {authors_to_query}")

            current_authors_to_query = copy.deepcopy(authors_to_query)
            authors_to_query.clear()

            # 1) For each author at this depth, run the query in parallel (asyncio.gather)
            tasks = []
            for author_id in current_authors_to_query:
                if author_id in authors_seen:
                    continue
                authors_seen.add(author_id)

                # Build a task to fetch the network for that author (e.g. co-authors).
                tasks.append(asyncio.create_task(fetch_author_links(author_id)))

            # 2) Wait for all tasks at this depth to finish
            #    and gather their partial results
            partial_results_list = await asyncio.gather(*tasks, return_exceptions=False)

            # 3) Process each partial result: collect co-author edges, schedule next depth
            for partial_result in partial_results_list:
                if isinstance(partial_result, Exception):
                    # Log or handle errors from a single task
                    app.logger.error(f"Error in parallel fetch: {partial_result}")
                    continue
                for record in partial_result:
                    end_author_id = int(record["end_author_id"])
                    if end_author_id not in authors_seen:
                        authors_to_query.append(end_author_id)
                    results.append(record)

            app.logger.info(
                f"Depth {start_depth} - Found {len(authors_to_query)} authors for next depth."
            )
            start_depth += 1

        # Now we have edges in 'results'. Next, we must fetch publication info for each edge,
        # also in parallel
        nodes = {}
        links = []

        # Ensure the starting node is in the graph
        nodes[start_author_id] = {
            "id": start_author_id,
            "label": starting_author["name"],
            "image": starting_author["image_url"],
        }

        # We'll group results by (start_id, end_id) so we can
        # do parallel calls to fetch publication ranks
        # at the next step.
        # results: [ { start_author_id, start_author_label, end_author_id, ...}, ... ]
        # We want to concurrently fetch publication rank info for each (start, end) pair.

        # 1) Create tasks to fill link details in parallel
        pub_tasks = []
        for record in results:
            pub_tasks.append(
                asyncio.create_task(
                    enrich_link_with_publications(
                        record["start_author_id"],
                        record["start_author_label"],
                        record["start_author_image_url"],
                        record["end_author_id"],
                        record["end_author_label"],
                        record["end_author_image_url"],
                        record["avg_conference_rank"],
                        record["avg_journal_rank"]
                    )
                )
            )

        # 2) Gather all link info
        enriched_links = await asyncio.gather(*pub_tasks, return_exceptions=False)

        for link_result in enriched_links:
            if isinstance(link_result, Exception):
                app.logger.error(f"Error in parallel publication fetch: {link_result}")
                continue
            link = link_result["link"]
            start_id = link["source"]
            end_id = link["target"]

            # Add start node
            if start_id not in nodes:
                nodes[start_id] = {
                    "id": start_id,
                    "label": link_result["start_label"],
                    "image": link_result["start_image"],
                }
            # Add end node
            if end_id not in nodes:
                nodes[end_id] = {
                    "id": end_id,
                    "label": link_result["end_label"],
                    "image": link_result["end_image"],
                }
            # Add link
            links.append(link)

        app.logger.info("Fetched %d nodes ann %d links.", len(nodes), len(links))

        return jsonify({
            "nodes": list(nodes.values()),
            "links": links
        })

    except ValueError as ve:
        app.logger.error("Invalid input: %s", ve)
        app.logger.error("Stack trace: %s", traceback.format_exc())
        return jsonify({"error": "Invalid start_author_id or depth"}), 400
    except Exception as e:
        app.logger.error("Unexpected error in generate_graph: %s", e)
        app.logger.error("Stack trace: %s", traceback.format_exc())
        return jsonify({"error": "Internal Server Error"}), 500


# ---------------------------
# ASYNC HELPER FUNCTIONS
# ---------------------------
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
    for rank_assoc in pub_ranks_result:
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
