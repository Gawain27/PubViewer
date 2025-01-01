import hashlib
from typing import Any, Dict, List, Optional, Union
from sqlalchemy.orm import Session, DeclarativeMeta
from sqlalchemy.sql import text
from cachetools import LRUCache, cached

class QueryBuilder:
    """
    A dynamic query builder for SQLAlchemy that supports table joins, dynamic WHERE clauses,
    ORDER BY, LIMIT, OFFSET, and result caching. Caching can be global, which is enough in most cases.
    Specific caches should override this class.
    """

    # Global cache shared across all QueryBuilder instances
    global_cache: LRUCache = LRUCache(maxsize=1000)

    def __init__(
        self,
        session: Session,
        entity_class: DeclarativeMeta,
        alias: str,
    ) -> None:
        """
        Initialize the QueryBuilder.

        :param session: SQLAlchemy session object.
        :param entity_class: ORM model class representing the table.
        :param alias: Alias for the primary table in the query.
        """
        self.session: Session = session
        self.entity_class: DeclarativeMeta = entity_class
        self.alias: str = alias
        self.conditions: List[str] = []  # WHERE conditions
        self.parameters: Dict[str, Any] = {}  # Named parameters for correct query execution
        self.order_by_clauses: List[str] = []  # ORDER BY clauses
        self.join_clause: str = ""  # JOIN statements
        self.custom_select: str = alias  # Fields to select, defaults to all fields via alias
        self.limit_value: Optional[int] = None  # LIMIT value
        self.offset_value: Optional[int] = None  # OFFSET value
        self.group_by_fields = []  # Store fields to group by
        self.having_conditions: List[str] = []  # HAVING conditions
        self.param_counter: int = 0  # Counter to create unique parameter names

    def _next_param_name(self, base: str) -> str:
        """Generate a unique parameter name."""
        hash_part = hashlib.md5(base.encode('utf-8')).hexdigest()[:10]
        param_name = f"{hash_part}{self.param_counter}"
        self.param_counter += 1
        return param_name

    def add_condition(
        self,
        operator: str,
        parameter: str,
        value: Any,
        custom: bool = False,
        condition_type: str = "AND",
        is_case_sensitive: bool = True,
    ) -> "QueryBuilder":
        """
        Add a condition to the query (AND/OR) with support for modular operators.

        :param operator: SQL operator to use (e.g., '=', '<', '>', 'LIKE').
        :param custom: If True, use a custom condition instead of parameter and value.
        :param parameter: Field name to filter (e.g., "u.name").
        :param value: Value to match.
        :param condition_type: Type of condition ("AND" or "OR").
        :param is_case_sensitive: Whether the condition should be case-sensitive.
        """

        if not is_case_sensitive and operator.upper() == "LIKE":
            parameter = f"LOWER({parameter})"
            value = value.lower()

        param_name = self._next_param_name(parameter)
        condition = f"{parameter} {operator} :{param_name}" if not custom else value

        if self.conditions:
            self.conditions.append(f"{condition_type} {condition}")
        else:
            self.conditions.append(condition)

        if not custom:
            self.parameters[param_name] = value

        return self

    def and_condition(self, parameter: str, value: Any, operator: str = "=", custom: bool = False, is_case_sensitive: bool = True) -> "QueryBuilder":
        """
        Add an AND condition to the query.

        :param parameter: Field name to filter (e.g., "u.name").
        :param value: Value to match.
        :param operator: SQL operator to use (e.g., '=', '<', 'LIKE').
        :param custom: If True, use a custom condition instead of parameter and value.
        """
        return self.add_condition(operator, parameter, value, custom, condition_type="AND", is_case_sensitive=is_case_sensitive)

    def or_condition(self, parameter: str, value: Any, operator: str = "=", custom: bool = False, is_case_sensitive: bool = True) -> "QueryBuilder":
        """
        Add an OR condition to the query.

        :param parameter: Field name to filter (e.g., "o.product_name").
        :param value: Value to match.
        :param operator: SQL operator to use (e.g., '=', '<', 'LIKE').
        :param custom: If True, use a custom condition instead of parameter and value.
        """
        return self.add_condition(operator, parameter, value, custom, condition_type="OR", is_case_sensitive=is_case_sensitive)

    def join(
            self,
            join_type: str,
            other: Union["QueryBuilder", DeclarativeMeta],
            join_alias: str,
            on_condition: Optional[str] = None,
            this_field: Optional[str] = None,
            other_field: Optional[str] = None,
    ) -> "QueryBuilderWithCTE":
        """
        Add a JOIN clause to the query.

        :param join_type: Type of join (e.g., "INNER", "LEFT").
        :param other: QueryBuilder instance or ORM model for the joined table.
        :param join_alias: Alias for the joined table.
        :param on_condition: Custom ON condition for the join (overrides field-based joins).
        :param this_field: Field in the primary table to join on (used if `on_condition` is None).
        :param other_field: Field in the joined table to join on (used if `on_condition` is None).
        """
        table_name = (
            other.entity_class.__tablename__
            if isinstance(other, QueryBuilder)
            else other.__tablename__
        )
        if on_condition:
            self.join_clause += f" {join_type.upper()} JOIN {table_name} {join_alias} ON {on_condition}"
        elif this_field and other_field:
            self.join_clause += f" {join_type.upper()} JOIN {table_name} {join_alias} ON " \
                                f"{self.alias}.{this_field} = {join_alias}.{other_field}"
        else:
            raise ValueError("Either 'on_condition' or both 'this_field' and 'other_field' must be provided.")
        return self

    def group_by(self, *fields: str) -> "QueryBuilder":
        """
        Add GROUP BY clause to the query.

        :param fields: Fields to group by (e.g., "u.id").
        """
        self.group_by_fields.extend(fields)
        return self

    def order_by(self, field: str, ascending: bool = True) -> "QueryBuilder":
        """
        Add an ORDER BY clause to the query.

        :param field: Field to sort by (e.g., "u.id").
        :param ascending: True for ASC, False for DESC.
        """
        self.order_by_clauses.append(f"{field} {'ASC' if ascending else 'DESC'}")
        return self

    def add_having_condition(
            self,
            operator: str,
            parameter: str,
            value: Any,
            custom: bool = False,
            condition_type: str = "AND",
            is_case_sensitive: bool = True
    ) -> "QueryBuilder":
        """
        Add a condition to the HAVING clause (AND/OR) with support for modular operators.

        :param operator: SQL operator to use (e.g., '=', '<', '>', 'LIKE').
        :param custom: If True, use a custom condition instead of parameter and value.
        :param parameter: Field name to filter (e.g., "u.name").
        :param value: Value to match.
        :param condition_type: Type of condition ("AND" or "OR").
        :param is_case_sensitive: Whether the condition should be case-sensitive.
        """
        if not is_case_sensitive and operator.upper() == "LIKE":
            parameter = f"LOWER({parameter})"
            value = value.lower()

        param_name = self._next_param_name(parameter)
        condition = f"{parameter} {operator} :{param_name}" if not custom else value

        if self.having_conditions:
            self.having_conditions.append(f"{condition_type} {condition}")
        else:
            self.having_conditions.append(condition)

        if not custom:
            self.parameters[param_name] = value

        return self

    def having_and(self, parameter: str, value: Any, operator: str = "=", custom: bool = False, is_case_sensitive: bool = True) -> "QueryBuilder":
        """
        Add an AND condition to the HAVING clause.

        :param parameter: Field name to filter (e.g., "u.name").
        :param value: Value to match.
        :param operator: SQL operator to use (e.g., '=', '<', 'LIKE').
        :param custom: If True, use a custom condition instead of parameter and value.
        """
        return self.add_having_condition(operator, parameter, value, custom, condition_type="AND", is_case_sensitive=is_case_sensitive)

    def having_or(self, parameter: str, value: Any, operator: str = "=", custom: bool = False, is_case_sensitive: bool = True) -> "QueryBuilder":
        """
        Add an OR condition to the HAVING clause.

        :param parameter: Field name to filter (e.g., "u.name").
        :param value: Value to match.
        :param operator: SQL operator to use (e.g., '=', '<', 'LIKE').
        :param custom: If True, use a custom condition instead of parameter and value.
        """
        return self.add_having_condition(operator, parameter, value, custom, condition_type="OR", is_case_sensitive=is_case_sensitive)

    def limit(self, limit: int) -> "QueryBuilder":
        """
        Add a LIMIT clause to the query.

        :param limit: Maximum number of results to return.
        """
        self.limit_value = limit
        return self

    def offset(self, offset: int) -> "QueryBuilder":
        """
        Add an OFFSET clause to the query.

        :param offset: Number of rows to skip.
        """
        self.offset_value = offset
        return self

    def select(self, custom_select: str) -> "QueryBuilder":
        """
        Customize the SELECT fields.

        :param custom_select: Fields to select (e.g., "u.id, u.name").
        """
        self.custom_select = custom_select
        return self

    @cached(cache=global_cache)
    def execute(self, close_session: bool = True) -> List[Dict[str, Any]]:
        """
        Execute the query and return the results as a list of dictionaries.

        :return: List of query results as dictionaries.
        """
        query_string = self.build_query_string()
        query = text(query_string)
        result = self.session.execute(query, self.parameters)

        # Convert rows to dictionaries
        result_set = [row._asdict() for row in result]
        if close_session:
            self.session.close()
        return result_set

    def build_query_string(self) -> str:
        """
        Construct the full SQL query string.

        :return: SQL query string.
        """
        base_query = f"SELECT {self.custom_select} FROM {self.entity_class.__tablename__} {self.alias}"
        where_clause = f" WHERE {' '.join(self.conditions)}" if self.conditions else ""
        order_by_clause = f" ORDER BY {', '.join(self.order_by_clauses)}" if self.order_by_clauses else ""
        group_by_clause = f" GROUP BY {', '.join(self.group_by_fields)}" if self.group_by_fields else ""
        limit_clause = f" LIMIT {self.limit_value}" if self.limit_value is not None else ""
        offset_clause = f" OFFSET {self.offset_value}" if self.offset_value is not None else ""
        having_clause = f" HAVING {' '.join(self.having_conditions)}" if self.having_conditions else ""
        return base_query + self.join_clause + where_clause + group_by_clause + having_clause + order_by_clause + limit_clause + offset_clause

