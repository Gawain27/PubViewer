import logging
import os

from flask import Flask, render_template
from flask import request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from com.gwngames.client.general.GeneralDetailOverview import GeneralDetailOverview
from com.gwngames.client.general.GeneralTableOverview import GeneralTableOverview
# Import your modules properly
from com.gwngames.config.Context import Context
from com.gwngames.server.query.QueryBuilder import QueryBuilder
from com.gwngames.server.query.queries.AuthorQuery import AuthorQuery
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

try:
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    ctx.set_session_maker(Session)
    print("Session maker has been successfully initialized.")
except Exception as e:
    print(f"Failed to initialize the session maker: {e}")
    raise

# Initialize files for caching
conf_reader = JsonReader(JsonReader.CONFIG_FILE_NAME)
ctx.set_config(conf_reader)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
#app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize the database
db = SQLAlchemy(app)
ctx.set_database(db)

@app.route('/')
def start_client():
    return render_template('template.html', content='<p>Discover authors and their groundbreaking publications.</p>', current_year=2024)

@app.route('/about')
def about():
    return render_template('template.html', content='<p> TODO.</p>', current_year=2024)

@app.route('/publications')
def publications():
    query_builder: QueryBuilder = PublicationQuery.build_overview_publication_query(ctx.get_session())

    table_component = GeneralTableOverview(query_builder, "Publications Overview", limit=100)
    table_component.entity_class = query_builder.entity_class
    table_component.alias = query_builder.alias
    table_component.add_string_filter("Title", "publication_title")
    table_component.add_string_filter(
        label="Conf. Rank",
        field="most_frequent_conference_rank",
        sql_expression="""
            CASE 
                WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank)
                ELSE 'N/A'
            END
        """
    )
    table_component.add_string_filter(
        label="SJR",
        field="most_frequent_journal_qrank",
        sql_expression="""
            CASE 
                WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)
                ELSE 'N/A'
            END
        """
    )

    table_component.add_row_method("View Publication Details", "publication_details")

    return render_template(
        "template.html",
        content=table_component.render()
    )

@app.route('/publication_details')
def publication_details():
    # Get parameters from the URL
    row_name = request.args.get('publication_title')

    query_builder = PublicationQuery.build_filtered_publication_query(ctx.get_session(), row_name)

    data_viewer = GeneralDetailOverview(
        query_builder,
        title_field="publication_title",
        description_field="description"
    )

    return render_template(
        'template.html',
        content=data_viewer.render()
    )
@app.route('/researchers')
def researchers():
    query_builder = AuthorQuery.build_author_overview_query(ctx.get_session())

    table_component = GeneralTableOverview(query_builder, "Researchers Overview", limit=100)
    table_component.alias = query_builder.alias
    table_component.entity_class = query_builder.entity_class
    table_component.add_string_filter("Name", "a.name")
    table_component.add_string_filter(
        label="Interest",
        field="interests",
        sql_expression="COALESCE(STRING_AGG(DISTINCT i.name, ', '), '-')"
    )
    table_component.add_string_filter(
        label="Avg. Conf. Rank",
        field="average_conference_rank",
        sql_expression="""
            CASE 
                WHEN COUNT(c.rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY c.rank)
                ELSE '-'
            END
        """
    )
    table_component.add_string_filter(
        label="Avg. SJR",
        field="average_q_rank",
        sql_expression="""
            CASE 
                WHEN COUNT(j.q_rank) > 0 THEN MODE() WITHIN GROUP (ORDER BY j.q_rank)
                ELSE 'N/A'
            END
        """
    )

    table_component.add_row_method("View Author Details", "researcher_detail")

    return render_template(
        "template.html",
        content=table_component.render()
    )

@app.route('/researcher_detail')
def researcher_detail():
    # Get parameters from the URL
    row_name = request.args.get('author_name')

    query_builder = AuthorQuery.build_author_query_with_filter(ctx.get_session(), author_name=row_name)

    data_viewer = GeneralDetailOverview(
        query_builder,
        title_field="author_name",
        description_field="homepage_url",
        image_field="image_url"
    )

    return render_template(
        'template.html',
        content=data_viewer.render()
    )

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=5000,debug=True)
