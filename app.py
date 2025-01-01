import copy
import json
import logging
import os
import sys
import traceback

from flask import Flask, render_template, jsonify, session
from flask import request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from com.gwngames.client.general.GeneralDetailOverview import GeneralDetailOverview
from com.gwngames.client.general.GeneralTableOverview import GeneralTableOverview
from com.gwngames.config.Context import Context
from com.gwngames.server.entity.base.Author import Author
from com.gwngames.server.query.QueryBuilder import QueryBuilder
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

# Initialize files for caching
conf_reader = JsonReader(JsonReader.CONFIG_FILE_NAME)
ctx.set_config(conf_reader)

db_url = conf_reader.get_value("db_url")
db_name = conf_reader.get_value("db_name")
db_user = conf_reader.get_value("db_user")
db_password = conf_reader.get_value("db_password")
db_port = conf_reader.get_value("db_port")

DATABASE_URL = f"postgresql+psycopg2://{db_user}:{db_password}@{db_url}:{db_port}/{db_name}"

logging.basicConfig(level=logging.DEBUG)  # Or INFO, WARNING, ERROR
logger = logging.getLogger(__name__)

try:
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    ctx.set_session_maker(Session)
    logger.info("Session maker has been successfully initialized.")
except Exception as e:
    logger.error(f"Failed to initialize the session maker: {e}")
    raise

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL

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

    query_builder = AuthorQuery.build_author_query_with_filter(ctx.get_session(), author_id=int(row_name))

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
    start_author_ids = request.args.get('value', None)

    if not start_author_ids:
        return "Error: Author ID is required", 400

    try:

        author_data: QueryBuilder = QueryBuilder(ctx.get_session(), Author, 'a')
        author_data.select("a.name")
        author_data.and_condition("", f"a.id IN ({start_author_ids})", custom=True)
        result = author_data.execute()

        start_author_labels = ""
        first = result.pop()
        start_author_labels += first.get("name")

        #  Handle additional authors
        for res in result:
            start_author_labels += f",{res.get("name")}"

        max_depth = int(JsonReader(JsonReader.CONFIG_FILE_NAME).get_value("max_generative_depth"))

        return render_template(
            "template.html",
            title="Author Network",
            content=render_template(
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


@app.route("/generate-graph", methods=["POST"])
def generate_graph():
    """
    Generates a tree-structured author graph starting from a given author ID.
    """
    try:
        # Log the incoming request
        app.logger.info("Received request to generate graph.")
        data = json.loads(request.get_data())
        app.logger.debug("Received JSON payload: %s", data)

        # Validate and log input parameters
        start_author_id = data.get("start_author_id")
        if not start_author_id:
            app.logger.error("Missing start_author_id in the request.")
            return jsonify({"error": "start_author_id is required"}), 400

        max_depth = data.get("depth")
        if not max_depth:
            app.logger.error("Missing depth in the request.")
            return jsonify({"error": "depth is required"}), 400

        app.logger.info(
            "Processing request with start_author_id=%s, depth=%s",
            start_author_id, max_depth)

        # Ensure valid types
        start_author_id = int(start_author_id)
        max_depth = int(max_depth)
        start_depth = 0
        authors_seen = set()
        authors_to_query = [start_author_id]

        # Initialize the database session
        net_session = Context().get_session()
        results = []

        # Query the author network
        app.logger.info("Starting author network traversal.")
        while start_depth < max_depth:
            app.logger.debug("Current depth: %s", start_depth)
            app.logger.debug("Authors to query at this depth: %s", authors_to_query)

            current_authors_to_query = copy.deepcopy(authors_to_query)
            authors_to_query.clear()

            for author_id in current_authors_to_query:
                if author_id in authors_seen:
                    app.logger.debug("Skipping already seen author_id: %s", author_id)
                    continue

                authors_seen.add(author_id)
                app.logger.info("Querying Author ID: %s at depth: %s", author_id, start_depth)

                query = AuthorQuery.build_author_group_query(
                    net_session,
                    author_id=author_id
                )

                try:
                    temp_results = query.execute(close_session=False)
                    app.logger.debug("Query returned %s records for author_id: %s", len(temp_results), author_id)
                except Exception as query_error:
                    app.logger.error("Error executing query for author_id %s: %s", author_id, query_error)
                    continue

                for record in temp_results:
                    end_author_id = int(record["end_author_id"])
                    if author_id == end_author_id:
                        app.logger.debug("Skipping self-loop for author_id: %s", author_id)
                        continue

                    authors_to_query.append(end_author_id)
                    results.append(record)

            app.logger.info("Depth %s traversal complete. Found %s new authors to query.", start_depth, len(authors_to_query))
            start_depth += 1

        authors_seen.clear()

        starting_author = QueryBuilder(net_session, Author, 'a').select('a.name, a.image_url').and_condition('a.id', start_author_id).execute()[0]

        app.logger.info("Author network traversal completed.")

        # Transform query results into nodes and links
        nodes = {}
        links = []

        app.logger.info("Starting graph generation.")

        # Ensure the starting author node is explicitly added
        if start_author_id not in nodes:
            app.logger.debug("Adding start author node: %s", start_author_id)
            nodes[start_author_id] = {
                "id": start_author_id,
                "label": starting_author.get("name"),
                "image": starting_author.get("image_url")
            }

        # Process the query results to build nodes and links
        for record in results:
            start_id, start_label = record["start_author_id"], record["start_author_label"]
            end_id, end_label = record["end_author_id"], record["end_author_label"]
            start_image, end_image = record["start_author_image_url"], record["end_author_image_url"]
            avg_conf_rank = record["avg_conference_rank"]
            avg_journal_rank = record["avg_journal_rank"]

            app.logger.debug("Processing record: %s", record)

            # Add start node if not already present
            if start_id not in nodes:
                app.logger.debug("Adding start node to graph: %s", start_id)
                nodes[start_id] = {
                    "id": start_id,
                    "label": start_label,
                    "image": start_image
                }

            # Add end node if not already present
            if end_id not in nodes:
                app.logger.debug("Adding end node to graph: %s", end_id)
                nodes[end_id] = {
                    "id": end_id,
                    "label": end_label,
                    "image": end_image
                }

            # Count the publication types total
            app.logger.debug("Querying pub ranks for nodes: %s - %s", start_id, end_id)
            publication_query = PublicationQuery.build_author_publication_query(net_session, start_id, end_id)
            rank_associations = publication_query.execute(close_session=False)

            app.logger.debug("Querying pub years for nodes: %s - %s", start_id, end_id)
            pub_year_query = PublicationQuery.build_author_publication_year_query(net_session, start_id, end_id)
            year_associations = pub_year_query.execute(close_session=False)

            link = {
                "source": start_id,
                "target": end_id,
                "avg_conf_rank": avg_conf_rank,
                "avg_journal_rank": avg_journal_rank
            }

            for rank_assoc in rank_associations:
                link[str(rank_assoc["rank_name"])] = rank_assoc["rank_total_pubs"]

            for year_assoc in year_associations:
                link[str(year_assoc["publication_year"])] = year_assoc["publication_count"]

            # Add a link between the start and end nodes
            app.logger.info("Processed link: %s", link)
            links.append(link)

        net_session.close()
        app.logger.info("Graph generation completed. Total nodes: %s, Total links: %s", len(nodes), len(links))

        app.logger.info("Nodes: %s", nodes)
        app.logger.info("Links: %s", links)
        return jsonify({"nodes": list(nodes.values()), "links": links})


    except ValueError as ve:
        app.logger.error("Invalid input: %s", ve)
        app.logger.error("Stack trace: %s", traceback.format_exc())
        return jsonify({"error": "Invalid start_author_id"}), 400
    except Exception as e:
        app.logger.error("Unexpected error in generate_graph: %s", e)
        app.logger.error("Stack trace: %s", traceback.format_exc())
        return jsonify({"error": "Internal Server Error"}), 500



if __name__ == '__main__':
    app.run(host="0.0.0.0",port=5000,debug=True)
