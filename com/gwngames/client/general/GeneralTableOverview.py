import logging
from typing import Optional

from quart import render_template, request

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
        self.entity_class: str
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

    async def render(self):
        """Render the table component with filters, buttons, and pagination."""
        offset = int(request.args.get("offset", 0))
        limit = self.limit
        logger.info(f"Rendering table with offset {offset}, limit {limit}")

        # Apply filters to the query builder
        for filter_el in self.filters:
            filter_value = request.args.get(filter_el["field_name"])

            is_bypass_rule = filter_el["filter_type"] == "integer"

            if filter_value or is_bypass_rule:
                logger.debug(f"Applying filter: {filter_el['field_name']} with value {filter_value}")
                if filter_el["filter_type"] == "string":
                    self.handle_string_filter(filter_el, filter_value)
                elif filter_el["filter_type"] == "integer":
                    self.handle_int_filter(filter_el)

        if request.args.get("apply_filters"):
            self.query_builder.offset(offset).limit(limit)
            rows = await self.query_builder.execute()
            logger.info(f"Filters applied, fetched {len(rows)} rows")
        else:
            rows = self.external_records
            self.external_records = []
            logger.info("No filters applied, using external records")

        count_query = QueryBuilder(Context().get_pool(), table_name=self.query_builder.table_name,
                                  alias=self.query_builder.alias)
        count_query.select(f"COUNT(DISTINCT {self.query_builder.alias}.id) AS count")
        count_query.conditions = self.query_builder.conditions
        count_query.parameters = self.query_builder.parameters
        count_query.join_clause = self.query_builder.join_clause
        count_query.param_counter = self.query_builder.param_counter
        count_query.having_conditions = self.query_builder.having_conditions
        count_query.group_by_fields = self.query_builder.group_by_fields
        count_records = await count_query.execute()
        count_result = sum(record["count"] for record in count_records)
        logger.info(f"Total records count: {count_result}")

        columns = list(rows[0].keys()) if rows else []

        filter_query_string = "&".join(
            f"{key}={value}" for key, value in request.args.items() if key not in ["offset", "limit"]
        )

        return await render_template(
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

    def handle_string_filter(self, filter_element, filter_value):
        """
        Handle string filters, applying them to the query builder.
        """
        field_name = filter_element["field_name"]
        is_case_sensitive = filter_element["is_case_sensitive"]
        logger.debug(
            f"Handling string filter for field: {field_name}, value: {filter_value}, case_sensitive: {is_case_sensitive}")

        if filter_element.get("is_aggregated", False):  # Check if field is aggregated
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

    def handle_int_filter(self, filter_element):
        from_value = request.args.get(f"{filter_element['field_name']}_from")
        to_value = request.args.get(f"{filter_element['field_name']}_to")
        logger.debug(f"Handling integer filter for field: {filter_element['field_name']}, from: {from_value}, to: {to_value}")

        if from_value is not None and from_value != '':
            self.query_builder.and_condition(
                f"{self.query_builder.alias}.{filter_element['field_name']}",
                int(from_value),
                operator=">="
            )
        if to_value is not None and to_value != '':
            self.query_builder.and_condition(
                f"{self.query_builder.alias}.{filter_element['field_name']}",
                int(to_value),
                operator="<="
            )
        JsonReader(self.query_builder.table_name).set_and_save("query",
                                                                          self.query_builder.build_query_string())
        logger.debug(f"Query after applying integer filter: {self.query_builder.build_query_string()}")