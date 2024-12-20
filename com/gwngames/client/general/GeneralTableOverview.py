import logging
from typing import Optional

from flask import render_template, request

from com.gwngames.config.Context import Context
from com.gwngames.server.query.QueryBuilder import QueryBuilder
from com.gwngames.utils.JsonReader import JsonReader

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


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
        self.external_records = []
        logger.info(f"Initialized GeneralTableOverview for {table_title} with limit {limit}")

    def add_filter(self, field_name: str, filter_type: str = "string", label: Optional[str] = None,
                   is_aggregated: bool = False, is_case_sensitive: bool = True):
        """
        Add a filter to the table.
        """
        self.filters.append({
            "field_name": field_name,
            "filter_type": filter_type,
            "label": label or field_name,
            "is_aggregated": is_aggregated,
            "is_case_sensitive": is_case_sensitive,
        })
        logger.debug(
            f"Added filter: {field_name}, type: {filter_type}, aggregated: {is_aggregated}, case_sensitive: {is_case_sensitive}")

    def add_row_method(self, label, endpoint_name):
        """
        Add a method to be triggered by a link in each row.
        """
        self.row_methods.append({"label": label, "endpoint": endpoint_name})
        logger.debug(f"Added row method: {label} -> {endpoint_name}")

    def render(self):
        """Render the table component with filters, buttons, and pagination."""
        offset = int(request.args.get("offset", 0))
        limit = self.limit
        logger.info(f"Rendering table with offset {offset}, limit {limit}")

        # Apply filters to the query builder
        for filter in self.filters:
            filter_value = request.args.get(filter["field_name"])

            is_bypass_rule = filter["filter_type"] == "integer"

            if filter_value or is_bypass_rule:
                logger.debug(f"Applying filter: {filter['field_name']} with value {filter_value}")
                if filter["filter_type"] == "string":
                    self.handle_string_filter(filter, filter_value)
                elif filter["filter_type"] == "integer":
                    self.handle_int_filter(filter, filter_value)

        if request.args.get("apply_filters"):
            self.query_builder.offset(offset).limit(limit)
            rows = self.query_builder.execute()
            logger.info(f"Filters applied, fetched {len(rows)} rows")
        else:
            rows = self.external_records
            self.external_records = []
            logger.info("No filters applied, using external records")

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
        logger.info(f"Total records count: {count_result}")

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

    def handle_string_filter(self, filter, filter_value):
        """
        Handle string filters, applying them to the query builder.
        """
        field_name = filter["field_name"]
        is_case_sensitive = filter["is_case_sensitive"]
        logger.debug(
            f"Handling string filter for field: {field_name}, value: {filter_value}, case_sensitive: {is_case_sensitive}")

        if filter.get("is_aggregated", False):  # Check if field is aggregated
            self.query_builder.having_and(
                field_name,
                f"%{filter_value}%",
                operator="LIKE",
                is_case_sensitive=is_case_sensitive
            )
        else:
            self.query_builder.and_condition(
                f"{self.query_builder.alias}.{field_name}",
                f"%{filter_value}%",
                operator="LIKE",
                is_case_sensitive=is_case_sensitive
            )

    def handle_int_filter(self, filter, filter_value):
        from_value = request.args.get(f"{filter['field_name']}_from")
        to_value = request.args.get(f"{filter['field_name']}_to")
        logger.debug(f"Handling integer filter for field: {filter['field_name']}, from: {from_value}, to: {to_value}")

        if from_value is not None and from_value != '':
            self.query_builder.and_condition(
                f"{self.query_builder.alias}.{filter['field_name']}",
                int(from_value),
                operator=">="
            )
        if to_value is not None and to_value != '':
            self.query_builder.and_condition(
                f"{self.query_builder.alias}.{filter['field_name']}",
                int(to_value),
                operator="<="
            )
        JsonReader(self.query_builder.entity_class.__name__).set_and_save("query",
                                                                          self.query_builder.build_query_string())
        logger.debug(f"Query after applying integer filter: {self.query_builder.build_query_string()}")