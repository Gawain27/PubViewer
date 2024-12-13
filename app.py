import logging
import os
import sys

from flask import Flask, render_template, jsonify, session
from flask import request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from com.gwngames.client.general.GeneralDetailOverview import GeneralDetailOverview
from com.gwngames.client.general.GeneralTableOverview import GeneralTableOverview
from com.gwngames.config.Context import Context

from com.gwngames.server.query.QueryBuilder import QueryBuilder
from com.gwngames.server.query.QueryBuilderWithCTE import RecursiveQueryBuilder
from com.gwngames.server.query.queries.AuthorQuery import AuthorQuery
from com.gwngames.server.query.queries.ConferenceQuery import ConferenceQuery
from com.gwngames.server.query.queries.JournalQuery import JournalQuery
from com.gwngames.server.query.queries.PublicationQuery import PublicationQuery
from com.gwngames.utils.JsonReader import JsonReader


class ExcludeFilter(logging.Filter):
    def filter(self, record):
        return not any(
            record.name.startswith(mod) for mod in ('httpx', 'httpcore', 'urllib3', 'selenium'))

# ----------------- REGION MAIN ---------

# Initialize context
ctx: Context = Context()
ctx.set_current_dir(os.getcwd())

DATABASE_URL = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres"

logging.basicConfig(level=logging.DEBUG)  # Or INFO, WARNING, ERROR
logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

try:
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    ctx.set_session_maker(Session)
    logger.info("Session maker has been successfully initialized.")
except Exception as e:
    logger.error(f"Failed to initialize the session maker: {e}")
    raise

# Initialize files for caching
conf_reader = JsonReader(JsonReader.CONFIG_FILE_NAME)
ctx.set_config(conf_reader)

logger.info(AuthorQuery.build_author_network_query(session, 1, 1).build_query_string())

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL

app.logger.addHandler(console_handler)
app.logger.setLevel(logging.DEBUG)

# Initialize the database
db = SQLAlchemy(app)
ctx.set_database(db)

@app.route('/')
def start_client():
    return render_template('template.html', content='<p>Discover authors and their groundbreaking publications.</p>', current_year=2024)

@app.route('/about')
def about():
    return render_template('template.html', content='<p> TODO.</p>', current_year=2025)

@app.route('/publications')
def publications():
    author = request.args.get('value', None)

    query_builder: QueryBuilder = PublicationQuery.build_overview_publication_query(ctx.get_session())
    table_component = GeneralTableOverview(query_builder, "Publications Overview", limit=100)

    if author is not None:
        author = f"%{author}%"
        query_builder.having_and("STRING_AGG(DISTINCT lower(a.name), ', ')", author, operator="LIKE", is_case_sensitive=False)
        query_builder.offset(0).limit(100)
        table_component.external_records = query_builder.execute()

    table_component.entity_class = query_builder.entity_class
    table_component.alias = query_builder.alias
    table_component.add_filter("Title", "string", "Title", is_case_sensitive=False)
    table_component.add_filter("CASE WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank) ELSE '-' END",
                               "string", "Conf. Rank", is_aggregated=True, is_case_sensitive=True)

    table_component.add_filter("CASE WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)ELSE '-' END",
                               "string", "Journal Rank", is_aggregated=True, is_case_sensitive=False)

    table_component.add_filter("publication_year", "integer", "Year")
    table_component.add_filter("STRING_AGG(DISTINCT lower(a.name), ', ')", "string", "Author", is_aggregated=True, is_case_sensitive=True)

    table_component.add_row_method("View Publication Details", "publication_details")

    return render_template(
        "template.html",
        content=table_component.render()
    )

@app.route('/publication_details')
def publication_details():
    # Get parameters from the URL
    row_name = request.args.get('id')

    query_builder = PublicationQuery.build_specific_publication_query(ctx.get_session(), row_name)

    data_viewer = GeneralDetailOverview(
        query_builder,
        title_field="Title",
        description_field="Description"
    )

    data_viewer.add_row_method("View Conference", "conferences", column_name="Conference")
    data_viewer.add_row_method("View Journal", "journals", column_name="Journal")

    return render_template(
        'template.html',
        content=data_viewer.render()
    )
@app.route('/researchers')
def researchers():
    query_builder = AuthorQuery.build_author_overview_query(ctx.get_session())

    table_component = GeneralTableOverview(query_builder, "Researchers Overview", limit=100, image_field="Image url")
    table_component.alias = query_builder.alias
    table_component.entity_class = query_builder.entity_class
    table_component.add_filter("Name", filter_type="string", label="Name", is_case_sensitive=False)
    table_component.add_filter("COALESCE(STRING_AGG(DISTINCT i.name, ', '), 'N/A')",
        filter_type="string", label="Interest", is_aggregated=True, is_case_sensitive=False)
    table_component.add_filter(
        "CASE WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank) ELSE '-' END",
        filter_type="string", label="Avg. Conf. Rank", is_aggregated=True, is_case_sensitive=True)
    table_component.add_filter("CASE WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank) ELSE '-' END",
        filter_type="string", label="Avg. Q. Rank", is_aggregated=True, is_case_sensitive=False
    )
    table_component.add_row_method("View Author Details", "researcher_detail")

    return render_template(
        "template.html",
        content=table_component.render()
    )

@app.route('/researcher_detail')
def researcher_detail():
    row_name = request.args.get('Author ID')

    query_builder = AuthorQuery.build_author_query_with_filter(ctx.get_session(), author_id=row_name)

    data_viewer = GeneralDetailOverview(
        query_builder,
        title_field="Name",
        description_field="Homepage",
        image_field="Image url"
    )

    data_viewer.add_row_method("View Publications", "publications", "Name")
    data_viewer.add_row_method("View Network", "author_network", "Author ID")

    return render_template(
        'template.html',
        content=data_viewer.render()
    )

@app.route('/conferences')
def conferences():
    acronym = request.args.get('value', None)

    query_builder = ConferenceQuery.getConferences(ctx.get_session())
    table_component = GeneralTableOverview(query_builder, "Conferences Overview", limit=100)

    if acronym is not None:
        author = f"%{acronym}%"
        query_builder.and_condition("Acronym", author, operator="LIKE", is_case_sensitive=False)
        query_builder.offset(0).limit(100)
        table_component.external_records = query_builder.execute()

    table_component.entity_class = query_builder.entity_class
    table_component.alias = query_builder.alias
    table_component.add_filter("Title", "string", "Title", is_case_sensitive=False)
    table_component.add_filter("Acronym", "string", "Acronym", is_case_sensitive=False)
    table_component.add_filter("Rank", "string", "Rank", is_case_sensitive=True)
    table_component.add_filter("Publisher", "string", "Publisher", is_case_sensitive=False)

    return render_template(
        "template.html",
        content=table_component.render()
    )

@app.route('/journals')
def journals():
    query_builder = JournalQuery.getJournals(ctx.get_session())

    table_component = GeneralTableOverview(query_builder, "Journals Overview", limit=100)
    table_component.entity_class = query_builder.entity_class
    table_component.alias = query_builder.alias
    table_component.add_filter("title", "string", "Title", is_case_sensitive=False)
    table_component.add_filter("q_rank", "string", "Rank", is_case_sensitive=True)
    table_component.add_filter("Year", "integer", "Year")
    return render_template(
        "template.html",
        content=table_component.render()
    )


# ------------------------------------------------
# NETWORKS
# ------------------------------------------------
@app.route('/author_network')
def author_network():
    """
    Renders the author network page, starting from a given author ID.
    """
    start_author_id = request.args.get('value', None)
    baseRedirectUrl = "/researcher_detail?name="
    return render_template("template.html", title="Author Network",
                           content=render_template("graph_component.html", start_id=start_author_id, base_path=baseRedirectUrl))


@app.route("/generate-graph", methods=["POST"])
def generate_graph():
    data = request.get_json()
    max_depth = int(data.get("max_depth", 5))
    start_author_id = int(data.get("start_author_id", 0))

    session = Context().get_session()
    query = AuthorQuery.build_author_network_query(session, start_author_id=start_author_id, max_depth=max_depth)
    results = query.execute()

    # Transform the results into nodes and links
    nodes = {}
    links = []

    for record in results:
        start_id, start_label = record["start_author_id"], record["start_author_label"]
        end_id, end_label = record["end_author_id"], record["end_author_label"]

        if start_id not in nodes:
            nodes[start_id] = {"id": start_id, "label": start_label}
        if end_id not in nodes:
            nodes[end_id] = {"id": end_id, "label": end_label}

        links.append({"source": start_id, "target": end_id, "label": record["author_total_pubs"]})

    return jsonify({"nodes": list(nodes.values()), "links": links})

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=5000,debug=True)
