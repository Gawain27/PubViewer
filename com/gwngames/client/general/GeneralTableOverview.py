import logging
import uuid
from typing import Optional, List, Dict

from quart import render_template, request
from com.gwngames.client.general.GeneralTableCache import store_query_builder
from com.gwngames.server.query.QueryBuilder import QueryBuilder

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class GeneralTableOverview:
    def __init__(
        self,
        query_builder: QueryBuilder,
        table_title: str,
        limit: int = 100,
        image_field: Optional[str] = None,
        enable_checkboxes: bool = False,
        url_fields=None,
    ):
        """
        Initialize the GeneralTableOverview.

        :param query_builder: An instance of QueryBuilder configured for the query.
        :param table_title:   Title of the table.
        :param limit:         Number of rows per page (for server-side).
        :param image_field:   The field in the row data containing the image URL.
        :param enable_checkboxes: Whether to display checkboxes in each row.
        """
        if url_fields is None:
            self.url_fields = []
        else:
            self.url_fields = url_fields
        self.query_builder: QueryBuilder = query_builder
        self.table_title: str = table_title
        self.limit: int = limit
        self.image_field: Optional[str] = image_field
        self.filters: List[Dict] = []
        self.row_methods: List[Dict] = []
        self.page_methods: List[Dict] = []
        self.external_records = []
        self.enable_checkboxes = enable_checkboxes

        # By default, might come from query_builder, but can be overridden later
        self.alias: Optional[str] = query_builder.alias
        self.entity_class: Optional[str] = query_builder.table_name

        # Generate a unique table_id to store/retrieve the QueryBuilder from cache
        self.table_id: str = str(uuid.uuid4())

        logger.info(
            f"Initialized GeneralTableOverview for '{table_title}' with limit={limit}, "
            f"checkboxes={enable_checkboxes}, table_id={self.table_id}"
        )

    def add_filter(
        self,
        field_name: str,
        filter_type: str = "string",
        label: Optional[str] = None,
        is_aggregated: bool = False,
        or_split: bool = False,
        equal: bool = False,
        int_like: bool = False
    ):
        """
        Add a filter to the table.
        """
        self.filters.append({
            "field_name": field_name,
            "filter_type": filter_type,
            "label": label or field_name,
            "is_aggregated": is_aggregated,
            "or_split": or_split,
            "equal": equal,
            "int_like": int_like
        })
        logger.debug(f"Added filter: {field_name}, type: {filter_type}, aggregated={is_aggregated}")

    def add_row_method(self, label: str, endpoint_name: str):
        """
        Add an action (button/link) that will appear in every row.
        """
        self.row_methods.append({"label": label, "endpoint": endpoint_name})
        logger.debug(f"Added row method: {label} -> {endpoint_name}")

    def add_page_method(self, label: str, endpoint_name: str):
        """
        Add a page-level button that will collect the ID of selected rows
        (via checkboxes) and pass them (comma-separated) to the given endpoint.
        """
        self.page_methods.append({"label": label, "endpoint": endpoint_name})
        logger.debug(f"Added page method: {label} -> {endpoint_name}")

    async def render(self):
        """
        Render the main HTML (template) that includes filters,
        checkboxes, pagination controls, etc.
        Initially, you can decide whether to run an initial query or not.
        Typically, we just render the skeleton or do a small initial fetch.
        """
        logger.info(f"Rendering table overview for '{self.table_title}' (table_id={self.table_id})")
        store_query_builder(self.table_id, self.query_builder, self.row_methods)

        # Optionally, we can fetch an initial page to show something by default:
        # or we can omit it and let the JavaScript fetch from /fetch_data
        # For demonstration, let's do an initial fetch:
        init_offset = 0
        columns = []

        # Apply filters to the query builder
        for filter_el in self.filters:
            filter_value = request.args.get(filter_el["field_name"])

            is_bypass_rule = filter_el["filter_type"] == "integer"

            if filter_value or is_bypass_rule:
                logger.debug(f"Applying filter: {filter_el['field_name']} with value {filter_value}")
                if filter_el["filter_type"] == "string":
                    self.handle_string_filter(filter_el, filter_value, filter_el.get("or_split"), filter_el.get("equal"))
                elif filter_el["filter_type"] == "integer":
                    self.handle_int_filter(filter_el)

        # Attempt a minimal initial fetch
        self.query_builder.offset(init_offset).limit(self.limit)
        init_rows = await self.query_builder.execute()

        unique_ids = set()
        filtered_rows = []
        for row in init_rows:
            first_property = next(iter(row))
            if row[first_property] not in unique_ids:
                filtered_rows.append(row)
                unique_ids.add(row[first_property])
        # Overwrite init_rows with the unique rows
        init_rows[:] = filtered_rows

        if init_rows:
            columns = list(init_rows[0].keys())

        # We'll just do a rough count here for the initial rendering
        count_query = self.query_builder.clone(no_limit=True, no_offset=True)
        count_query.order_by_fields = []
        count_query = QueryBuilder(count_query.pool, f"({count_query.build_query_string()})", "count")
        count_query.parameters = self.query_builder.parameters
        count_query.select("COUNT(*) AS count")
        count_data = await count_query.execute()
        total_count = sum(row["count"] for row in count_data)

        return await render_template(
            "general_table_overview.html",
            table_id=self.table_id,
            table_title=self.table_title,
            filters=self.filters,
            row_methods=self.row_methods,
            page_methods=self.page_methods,
            image_field=self.image_field,
            enable_checkboxes=self.enable_checkboxes,
            initial_rows=init_rows,
            columns=columns,
            offset=init_offset,
            limit=self.limit,
            total_count=total_count,
            url_fields=self.url_fields,
        )

    def handle_string_filter(self, filter_element, filter_value, or_split, equal):
        """
        Handle string filters, applying them to the query builder.
        """
        if filter_element["int_like"] and not filter_value.isnumeric():
            return

        field_name = filter_element["field_name"]
        logger.debug(
            f"Handling string filter for field: {field_name}, value: {filter_value}")

        # Split filter_value by comma, trim whitespace, and iterate over each value
        filter_values = [value.strip() for value in filter_value.split(',')]
        is_aggregated = filter_element.get("is_aggregated", False)

        if or_split:
            or_conditions = []
            for value in filter_values:
                if is_aggregated:  # Check if field is aggregated
                    condition = (field_name, "ILIKE" if not equal else "=" , f"%{value}%" if not equal else f"{value}", False)
                    or_conditions.append(condition)
                else:
                    condition = (f"{field_name}", "ILIKE" if not equal else "=", f"%{value}%" if not equal else f"{value}", False)
                    or_conditions.append(condition)

            self.query_builder.add_nested_conditions(
                conditions=or_conditions,
                operator_between_conditions="OR",
                condition_type="AND",
                is_having=is_aggregated
            )
        else:
            for value in filter_values:
                if is_aggregated:  # Check if field is aggregated
                    self.query_builder.having_and(
                        field_name,
                        f"%{value}%" if not equal else f"{value}",
                        operator="ILIKE" if not equal else "="
                    )
                else:
                    self.query_builder.and_condition(
                        f"{field_name}",
                        f"%{value}%" if not equal else f"{value}",
                        operator="ILIKE" if not equal else "="
                    )


    def handle_int_filter(self, filter_element):
        from_value = request.args.get(f"{filter_element['field_name']}_from")
        to_value = request.args.get(f"{filter_element['field_name']}_to")
        logger.debug(f"Handling integer filter for field: {filter_element['field_name']}, from: {from_value}, to: {to_value}")

        if from_value is not None and from_value != '':
            self.query_builder.and_condition(
                f"{filter_element['field_name']}",
                int(from_value),
                operator=">="
            ).and_condition(
                f"{filter_element['field_name']}",
                int(1950),
                operator=">="
            )
        if to_value is not None and to_value != '':
            self.query_builder.and_condition(
                f"{filter_element['field_name']}",
                int(to_value),
                operator="<="
            ).and_condition(
                f"{filter_element['field_name']}",
                int(1950),
                operator=">="
            )