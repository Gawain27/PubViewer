import asyncio
import logging
import os
import threading
import time
import schedule
import traceback
from collections import defaultdict

from psycopg.rows import dict_row
# psycopg3 async usage
from psycopg_pool import AsyncConnectionPool
from quart import Quart, render_template, jsonify, request

from com.gwngames.client.general.GeneralDetailOverview import GeneralDetailOverview
from com.gwngames.client.general.GeneralTableCache import get_query_builder
from com.gwngames.client.general.GeneralTableOverview import GeneralTableOverview
from com.gwngames.config.Context import Context
from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.entity.variant.scholar.GoogleScholarAuthor import GoogleScholarAuthor
from com.gwngames.server.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication
from com.gwngames.server.query.ColumnUpdater import update_authors_column
from com.gwngames.server.query.OrderFunctions import handle_order_by
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

        schedule.every(10).minutes.do(update_authors_column, pool)

        print("Starting the query scheduler...")

        def run_schedule():
            while True:
                schedule.run_pending()
                time.sleep(60)

        threading.Thread(target=run_schedule, daemon=True).start()
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
        author_count_query = QueryBuilder(pool, GoogleScholarAuthor.__tablename__, 'g', cache_results=False).select('COUNT(*)')
        publication_key_query = QueryBuilder(pool, GoogleScholarPublication.__tablename__, 'g', cache_results=False).select('g.publication_key').group_by("g.publication_key")
        publication_count_query = QueryBuilder(pool, "("+publication_key_query.build_query_string()+")", 'q', cache_results=False).select('COUNT(*)')

        author_count_result = await author_count_query.execute()
        publication_count_result = await publication_count_query.execute()

        author_count = list(author_count_result)[0]["count"]
        publication_count = list(publication_count_result)[0]["count"]

        return await render_template(
            'template.html',
            content= await render_template('homepage.html', publication_count=publication_count, author_count=author_count),
        )
    except Exception as e:
        app.logger.error(f"Error in start_client: {e}")
        return await render_template(
            'template.html',
            content='<p>Error loading home page. Please try again later.</p>',
        )

# -------------------- REGION APPLICATION -------------------------------

@app.get('/about')
async def about():
    return await render_template(
        'template.html',
        content=await render_template("about.html"),
    )


@app.get('/publications')
async def publications():
    """
    Example: This route continues to rely on the same logic,
    but internally you must ensure your QueryBuilder calls use async psycopg,
    not synchronous SQLAlchemy.
    """
    journal = None
    conference = None
    author = request.args.get('Author ID', None)
    if author is None:
        author = request.args.get('value', None)
    if author is None:
        journal = request.args.get("Journal ID", None)
    if author is None and journal is None:
        conference = request.args.get("Conf ID", None)

    query_builder: QueryBuilder = PublicationQuery.build_overview_publication_query(ctx.get_pool())
    table_component = GeneralTableOverview(query_builder, "Publications Overview", limit=ctx.get_config().get_value("max_overview_rows"), enable_checkboxes=True)

    if author is not None:
        author_values = ','.join([val.strip() for val in author.split(',') if val.strip()])
        author_values = await (QueryBuilder(ctx.get_pool(), Author.__tablename__, "a")
                               .and_condition("", f"a.id IN ({author_values})", custom=True)
                               .select("a.name").execute())

        # Build conditions for each author value.
        # Each condition is a tuple of (parameter, operator, value, custom),
        conditions = []
        for val in author_values:
            like_val = f"%{val['name']}%"
            conditions.append(
                ("p.authors", "ILIKE", like_val, False)
            )

        # Add them as a nested condition in the HAVING clause:
        # AND( param LIKE %val1% OR param LIKE %val2% OR ...)
        query_builder.add_nested_conditions(
            conditions=conditions,
            operator_between_conditions="OR",
            condition_type="AND",
            is_having=True
        )

        query_builder.offset(0).limit(ctx.get_config().get_value("max_overview_rows"))

    if conference is not None:
        conference = ','.join([val.strip() for val in conference.split(',') if val.strip()])
        pubs_ids = await ConferenceQuery.build_publications_from_conferences_query(pool, conference).execute()
        conditions = []
        if len(pubs_ids) == 0:
            conditions.append(("p.ID", "=", "0", False))
        for val in pubs_ids:
            conditions.append(
                ("p.ID", "=", val['id'], False)
            )

        query_builder.add_nested_conditions(
            conditions=conditions,
            operator_between_conditions="OR",
            condition_type="AND",
            is_having=True
        )

        query_builder.offset(0).limit(ctx.get_config().get_value("max_overview_rows"))

    if journal is not None:
        journal = ','.join([val.strip() for val in journal.split(',') if val.strip()])
        pubs_ids = await JournalQuery.build_publications_from_journals_query(pool, journal).execute()

        conditions = []
        if len(pubs_ids) == 0:
            conditions.append(("p.id", "=", "0", False))
        for val in pubs_ids:
            conditions.append(
                ("p.ID", "=", val['id'], False)
            )

        query_builder.add_nested_conditions(
            conditions=conditions,
            operator_between_conditions="OR",
            condition_type="AND",
            is_having=True
        )

        query_builder.offset(0).limit(ctx.get_config().get_value("max_overview_rows"))

    table_component.entity_class = query_builder.table_name
    table_component.alias = query_builder.alias
    table_component.add_filter("p.id", "string", "Pub. ID (OR)", or_split=True, equal=True)
    table_component.add_filter("p.title", "string", "Title (OR)", or_split=True)
    table_component.add_filter(
        "c.rank",
        "string", "Conf. Rank (OR)", is_aggregated=False, or_split=True, equal=True
    )
    table_component.add_filter(
        "j.q_rank",
        "string", "Journal Rank (OR)", is_aggregated=False, or_split=True
    )
    table_component.add_filter("p.publication_year", "integer", "Year")
    table_component.add_filter(
        "p.authors",
        "string",
        "Author (AND)",
        is_aggregated=False,
        or_split=False
    )

    table_component.add_row_method("View Publication Details", "publication_details")
    table_component.add_row_method("View Authors", "researchers")

    table_component.add_page_method("View Combined Authors", "researchers")

    return await render_template(
        "template.html",
        content=await table_component.render(),
        popup=await render_template("popup.html")
    )


@app.get('/publication_details')
async def publication_details():
    row_name = request.args.get('ID')
    if row_name is None:
        row_name = request.args.get('value')

    query_builder = PublicationQuery.build_specific_publication_query(ctx.get_pool(), row_name)

    data_viewer = GeneralDetailOverview(
        query_builder,
        title_field="Title",
        description_field="Description",
        url_fields=["Scholar URL"]
    )

    data_viewer.add_row_method("View Conference", "conferences", column_name="Conference")
    data_viewer.add_row_method("View Journal", "journals", column_name="Journal")

    return await render_template(
        'template.html',
        content=await data_viewer.render()
    )


@app.get('/researchers')
async def researchers():
    journal = None
    conference = None
    pubs = None
    author_id = request.args.get('Author ID')
    if author_id is None:
        pubs = request.args.get('ID', None)
    if pubs is None:
        pubs = request.args.get('value', None)
    if pubs is None:
        journal = request.args.get('Journal ID', None)
    if pubs is None and journals is None:
        conference = request.args.get('Conf ID', None)

    query_builder: QueryBuilder = AuthorQuery.build_author_overview_query(ctx.get_pool())

    table_component = GeneralTableOverview(query_builder, "Researchers Overview",
                                           limit=ctx.get_config().get_value("max_overview_rows"),
                                           image_field="Image url",
                                           enable_checkboxes=True
                                           )

    if pubs is not None:
        pub_ids = ','.join([val.strip() for val in pubs.split(',') if val.strip()])

        author_names = await AuthorQuery.build_authors_from_pub_query(pool, pub_ids).execute()
        conditions = []
        if len(author_names) == 0:
            conditions.append(("ab.Name", "=", "0", False))
        for val in author_names:
            conditions.append(
                ("ab.Name", "=", val['name'], False)
            )

        query_builder.add_nested_conditions(
            conditions=conditions,
            operator_between_conditions="OR",
            condition_type="AND",
            is_having=False
        )

        query_builder.offset(0).limit(ctx.get_config().get_value("max_overview_rows"))

    if conference is not None:
        conference = ','.join([val.strip() for val in conference.split(',') if val.strip()])
        author_names = await ConferenceQuery.build_authors_from_conferences_query(pool, conference).execute()
        conditions = []
        if len(author_names) == 0:
            conditions.append(("ab.Name", "=", "0", False))
        for val in author_names:
            conditions.append(
                ("ab.Name", "=", val['name'], False)
            )

        query_builder.add_nested_conditions(
            conditions=conditions,
            operator_between_conditions="OR",
            condition_type="AND",
            is_having=False
        )

        query_builder.offset(0).limit(ctx.get_config().get_value("max_overview_rows"))

    if journal is not None:
        journal = ','.join([val.strip() for val in journal.split(',') if val.strip()])
        author_names = await JournalQuery.build_authors_from_journals_query(pool, journal).execute()

        conditions = []
        if len(author_names) == 0:
            conditions.append(("ab.Name", "=", "0", False))
        for val in author_names:
            conditions.append(
                ("ab.Name", "=", val['name'], False)
            )

        query_builder.add_nested_conditions(
            conditions=conditions,
            operator_between_conditions="OR",
            condition_type="AND",
            is_having=False
        )

        query_builder.offset(0).limit(ctx.get_config().get_value("max_overview_rows"))

    if author_id is not None:
        author_id = request.args.get('Author ID')
        coauthors = await AuthorQuery.build_co_authors_query(ctx.get_pool(), author_id).execute()
        coauthor_ids = []
        for val in coauthors:
            coauthor_ids.append(str(val['id']))
        coauthor_ids = ','.join(coauthor_ids)
        numbers = coauthor_ids.split(',')
        coauthor_ids = ','.join(f"({num})" for num in numbers)
        query_builder.join("INNER", f"(VALUES {coauthor_ids})", "co(id)", on_condition="co.id = ab.id")

    table_component.query_builder = query_builder
    table_component.alias = query_builder.alias
    table_component.entity_class = query_builder.table_name
    table_component.add_filter("ab.id", filter_type="string", label="Author ID (OR)", or_split=True, equal=True)
    table_component.add_filter("ab.Name", filter_type="string", label="Name (OR)", or_split=True)
    table_component.add_filter(
        "i.interests",
        filter_type="string", label="Interest (AND)", is_aggregated=False, or_split=False
    )
    table_component.add_filter(
        "fc.freq_conf_rank",
        filter_type="string", label="Frequent Conf. Rank (OR)", is_aggregated=False, or_split=True, equal=True
    )
    table_component.add_filter(
        "fj.freq_journal_rank",
        filter_type="string", label="Frequent Journ. Rank (OR)", is_aggregated=False, or_split=True
    )
    table_component.add_row_method("View Author Details", "researcher_detail")
    table_component.add_row_method("View Publications", "publications")
    table_component.add_row_method("View Co-Authors", "researchers")
    table_component.add_page_method("View Combined Network", "author_network")
    table_component.add_page_method("View Combined Publications", "publications")

    return await render_template(
        "template.html",
        content=await table_component.render(),
        popup=await render_template("popup.html")
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
        image_field="Image url",
        url_fields=["Scholar Profile", "Homepage"]
    )

    data_viewer.add_row_method("View Publications", "publications", "Author ID")
    data_viewer.add_row_method("View Network", "author_network", "Author ID")

    return await render_template(
        'template.html',
        content=await data_viewer.render(),
    )


@app.get('/conferences')
async def conferences():
    acronym = request.args.get('value', None)

    query_builder: QueryBuilder = ConferenceQuery.get_conferences(ctx.get_pool())
    table_component = GeneralTableOverview(query_builder, "Conferences Overview", limit=ctx.get_config().get_value("max_overview_rows"),
                                           enable_checkboxes=True, url_fields=["Dblp Link"])

    if acronym is not None:
        author = f"%{acronym}%"
        query_builder.and_condition("Acronym", author, operator="LIKE", is_case_sensitive=False)
        query_builder.offset(0).limit(100)
        table_component.external_records = await query_builder.execute()

    table_component.entity_class = query_builder.table_name
    table_component.alias = query_builder.alias
    table_component.add_filter("c.id", filter_type="string", label="ID (OR)", or_split=True, equal=True)
    table_component.add_filter("c.title", "string", "Title (OR)", or_split=True)
    table_component.add_filter("c.acronym", "string", "Acronym (OR)", or_split=True)
    table_component.add_filter("c.rank", "string", "Rank (OR)", or_split=True, equal=True)
    table_component.add_filter("c.publisher", "string", "Publisher")

    table_component.add_row_method("View Publications", "publications")
    table_component.add_row_method("View Authors", "researchers")
    table_component.add_page_method("View Conferences Network", "conference_network")

    return await render_template(
        "template.html",
        content=await table_component.render(),
        popup=await render_template("popup.html")
    )


@app.get('/journals')
async def journals():
    query_builder: QueryBuilder = JournalQuery.get_journals(ctx.get_pool())

    table_component = GeneralTableOverview(query_builder, "Journals Overview", limit=ctx.get_config().get_value("max_overview_rows"),
                                           enable_checkboxes=True, url_fields=["Journal Page"])
    table_component.entity_class = query_builder.table_name
    table_component.alias = query_builder.alias
    table_component.add_filter("j.id", filter_type="string", label="ID (OR)", or_split=True, equal=True)
    table_component.add_filter("j.title", "string", "Title (OR)", or_split=True)
    table_component.add_filter("j.q_rank", "string", "Rank (OR)", or_split=True, equal=True)
    table_component.add_filter("j.year", "integer", "Year")

    table_component.add_row_method("View Publications", "publications")
    table_component.add_row_method("View Authors", "researchers")
    table_component.add_page_method("View Journals Network", "journal_network")

    return await render_template(
        "template.html",
        content=await table_component.render(),
        popup=await render_template("popup.html")
    )


@app.get('/author_network')
async def author_network():
    """
    Renders the author network page, starting from a given author ID.
    """
    start_author_ids = request.args.get('value', None)

    if not start_author_ids:
        return "Error: Author ID is required", 400

    numbers = start_author_ids.split(',')
    start_author_ids = ','.join(f"({num})" for num in numbers)

    return await render_network(start_author_ids)


@app.get('/journal_network')
async def journal_network():
    """
    Renders the author network page, starting from a given author ID.
    """
    journal_ids = request.args.get('value', None)

    if not journal_ids:
        return "Error: Journal IDs are required", 400

    start_authors = await JournalQuery.build_authors_from_journals_query(pool, journal_ids).execute()
    start_author_ids = ','.join([str(val['id']) for val in start_authors if val['id']])

    if not start_author_ids:
        return "No authors found for the selected journal(s)", 200

    return await render_network(start_author_ids)


@app.get('/conference_network')
async def conference_network():
    """
    Renders the conference network page, starting from a given conference ID.
    """
    conference_ids = request.args.get('value', None)

    if not conference_ids:
        return "Error: Conference IDa are required", 400

    start_authors = await ConferenceQuery.build_authors_from_conferences_query(pool, conference_ids).execute()
    start_author_ids = ','.join([str(val['id']) for val in start_authors if val['id']])

    if not start_author_ids:
        return "No authors found for the selected conference(s)", 200

    return await render_network(start_author_ids)

async def render_network(start_author_ids):
    try:
        author_data: QueryBuilder = QueryBuilder(ctx.get_pool(), Author.__tablename__, 'a')
        author_data.select("a.name")
        #author_data.and_condition("", f"a.id IN ({start_author_ids})", custom=True)
        author_data.join("INNER", f"(VALUES {start_author_ids})", "id_authors(id)", on_condition="a.id = id_authors.id")
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

    org = author["Organization"]
    return jsonify( {"author_data":
        {
            "Organization": f"{org}",
            "hIndex": author["H Index"],
            "i10Index": author["I10 Index"],
            "citesTotal": author["Total Cites"],
            "pubTotal": author["Publications Found"],
            "avg_conference_rank": author["Frequent Conf. Rank"],
            "avg_journal_rank": author["Frequent Journal Rank"]
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

    order_type = request.args.get("order_type")
    order_column = request.args.get("order_column")

    if order_column != "" and order_type != "":
        qb.order_by_clauses = []
        handle_order_by(qb, order_column, order_type)

    form = await request.form
    offset = int(form.get("offset", 0))
    limit = int(form.get("limit", 100))

    qb.offset(offset).limit(limit)

    rows = await qb.execute()

    # Count total rows
    count_query = qb.clone(no_limit=True, no_offset=True)
    count_query = QueryBuilder(count_query.pool, f"({count_query.build_query_string()})", "count")
    count_query.select(f"COUNT(*) AS count")
    count_query.parameters = qb.parameters
    count_query.order_by_fields = []
    app.logger.info("params: " + str(count_query.parameters))
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
        start_author_id = str(data["start_author_id"])
        max_depth = int(data["depth"])
        max_tuple_per_query = int(conf_reader.get_value("max_tuple_per_query"))

        # BFS variables
        start_depth = 0
        authors_seen = set()
        authors_to_query = start_author_id.split(',')
        edges = []
        nodes = {}

        # 0 - Starting authors info (in case of no results)

        numbers = start_author_id.split(',')
        start_author_id = ','.join(f"({num})" for num in numbers)
        sql_authors = await (QueryBuilder(ctx.get_pool(), Author.__tablename__, 'a').select('a.id, a.name, a.image_url')
                             #.and_condition("", f"a.id IN ({start_author_id})", custom=True)
                            .join("INNER", f"(VALUES {start_author_id})", "id_author(id)", on_condition="a.id = id_author.id")
                             .execute())

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
            chunk_size = max_tuple_per_query
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

        # Ensure starting authors are in the graph
        for author in sql_authors:
            if author["id"] not in nodes:
                nodes[author["id"]] = {
                    "id": author["id"],
                    "label": author["name"],
                    "image": author["image_url"],
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
            chunk_size = max_tuple_per_query
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

        # ---------------------------
        # 5) Finalize node rankings
        # ---------------------------
        total_nodes_ids = []
        for node_id, node_data in nodes.items():
            total_nodes_ids.append(node_id)

        total_nodes_ids = ','.join(f"({num})" for num in total_nodes_ids)
        nodes_full_data = await AuthorQuery.build_author_overview_query(pool).join(
            "INNER", f"(VALUES {total_nodes_ids})", "totids(id)", on_condition="totids.id = ab.id"
        ).execute()

        for node_id, node_data in nodes.items():
            author_data = next((x for x in nodes_full_data if x["Author ID"] == node_id), None)
            freq_conf_rank = author_data["Frequent Conf. Rank"]
            freq_jour_rank = author_data["Frequent Journal Rank"]

            node_data["freq_conf_rank"] = freq_conf_rank
            node_data["freq_journal_rank"] = freq_jour_rank

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


if __name__ == '__main__':
    # For dev/test:
    # Note that Quart’s debug reloader can cause multiple event loops.
    # In production, run with a proper ASGI server (e.g., hypercorn or uvicorn).
    # I use hypercorn because EZ, but there is really no valid distinction
    app.run(host="0.0.0.0", port=5000, debug=True)
