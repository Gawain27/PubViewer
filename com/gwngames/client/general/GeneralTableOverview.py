from typing import Optional

from flask import render_template, request

from com.gwngames.config.Context import Context
from com.gwngames.server.query.QueryBuilder import QueryBuilder


class GeneralTableOverview:
    def __init__(self, query_builder, table_title, limit=100, image_field=None):
        """
        Initialize the GeneralTableOverview.

        :param query_builder: An instance of QueryBuilder configured for the query.
        :param table_title: Title of the table.
        :param limit: Number of rows per page.
        :param image_field: The field in the row data containing the image URL.
        """
        self.query_builder: QueryBuilder = query_builder
        self.entity_class = None
        self.alias = None
        self.table_title = table_title
        self.limit = limit
        self.image_field = image_field
        self.filters = []
        self.row_methods = []

    def add_row_method(self, label, endpoint_name):
        """
        Add a method to be triggered by a link in each row.

        :param label: The label of the button/link.
        :param endpoint_name: The Flask endpoint to call when the link is clicked.
        """
        self.row_methods.append({"label": label, "endpoint": endpoint_name})

    def render(self):
        """Render the table component with filters, buttons, and pagination."""
        offset = int(request.args.get("offset", 0))
        limit = self.limit

        rows = []
        if request.args.get("apply_filters"):
            self.query_builder.offset(offset).limit(limit)
            rows = self.query_builder.execute()

        countQuery = QueryBuilder(Context().get_session(), entity_class=self.query_builder.entity_class,
                                  alias=self.query_builder.alias)
        countQuery.select(f"COUNT(DISTINCT {self.query_builder.alias}.id) AS count")
        countQuery.conditions = self.query_builder.conditions
        countQuery.parameters = self.query_builder.parameters
        countQuery.join_clause = self.query_builder.join_clause
        countQuery.param_counter = self.query_builder.param_counter
        countQuery.having_conditions = self.query_builder.having_conditions
        countQuery.group_by_fields = self.query_builder.group_by_fields
        count_records = countQuery.execute()
        count_result = sum(record["count"] for record in count_records)

        columns = list(rows[0].keys()) if rows else []

        filter_query_string = "&".join(
            f"{key}={value}" for key, value in request.args.items() if key not in ["offset", "limit"]
        )

        return render_template(
            "general_table_overview.html",
            table_title=self.table_title,
            rows=rows,
            columns=columns,
            filters=self.filters,
            row_methods=self.row_methods,
            offset=offset,
            limit=limit,
            total_count=count_result,
            filter_query_string=filter_query_string,
            image_field=self.image_field,
        )
