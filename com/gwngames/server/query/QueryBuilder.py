import hashlib
import logging
from typing import Any, Dict, List, Optional, Union

import cachetools
from psycopg_pool import AsyncConnectionPool

# we must carefully handle concurrency or use an async-safe cache library.
# but be aware of potential concurrency issues in a high-traffic environment.
# Alternatively, maybe consider an async Redis-based cache or similar approach?

class QueryBuilder:
    """
    A dynamic query builder that builds raw SQL statements for use with an async psycopg3 connection/pool.
    """

    # Global cache shared across all QueryBuilder instances
    global_cache: cachetools.LRUCache = cachetools.LRUCache(maxsize=1000)

    def __init__(
            self,
            pool: AsyncConnectionPool,
            table_name: str,
            alias: str,
    ) -> None:
        """
        :param pool: An async psycopg3 connection pool.
        :param table_name: The physical table name (i.e., MyTable.__tablename__ in SQLAlchemy).
        :param alias: An alias for the table in the SQL query.
        """
        self.pool = pool
        self.table_name: str = table_name
        self.alias: str = alias

        # Query builder data
        self.conditions: List[str] = []
        self.parameters: Dict[str, Any] = {}
        self.order_by_clauses: List[str] = []
        self.join_clause: str = ""
        self.custom_select: str = alias  # default: "SELECT alias" => all columns from alias
        self.limit_value: Optional[int] = None
        self.offset_value: Optional[int] = None
        self.group_by_fields: List[str] = []
        self.having_conditions: List[str] = []
        self.param_counter: int = 0
        self.logger = logging.getLogger(self.__class__.__name__)

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
        Add a condition to the query (AND/OR).
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

    def and_condition(self, parameter: str, value: Any, operator: str = "=", custom: bool = False,
                      is_case_sensitive: bool = True) -> "QueryBuilder":
        return self.add_condition(operator, parameter, value, custom, "AND", is_case_sensitive)

    def or_condition(self, parameter: str, value: Any, operator: str = "=", custom: bool = False,
                     is_case_sensitive: bool = True) -> "QueryBuilder":
        return self.add_condition(operator, parameter, value, custom, "OR", is_case_sensitive)

    def join(
            self,
            join_type: str,
            other: Union["QueryBuilder", str],
            join_alias: str,
            on_condition: Optional[str] = None,
            this_field: Optional[str] = None,
            other_field: Optional[str] = None,
    ) -> "QueryBuilder":
        """
        Add a JOIN clause to the query.

        :param other: If it's a QueryBuilder, we take its table_name; if it's a string, we assume it's a table name.
        """
        if isinstance(other, QueryBuilder):
            table_name = other.table_name
        else:
            table_name = other

        if on_condition:
            self.join_clause += f" {join_type.upper()} JOIN {table_name} {join_alias} ON {on_condition}"
        elif this_field and other_field:
            self.join_clause += (
                f" {join_type.upper()} JOIN {table_name} {join_alias} "
                f"ON {self.alias}.{this_field} = {join_alias}.{other_field}"
            )
        else:
            raise ValueError("Either 'on_condition' or both 'this_field' and 'other_field' must be provided.")
        return self

    def group_by(self, *fields: str) -> "QueryBuilder":
        self.group_by_fields.extend(fields)
        return self

    def order_by(self, field: str, ascending: bool = True) -> "QueryBuilder":
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

    def having_and(self, parameter: str, value: Any, operator: str = "=", custom: bool = False,
                   is_case_sensitive: bool = True) -> "QueryBuilder":
        return self.add_having_condition(operator, parameter, value, custom, "AND", is_case_sensitive)

    def having_or(self, parameter: str, value: Any, operator: str = "=", custom: bool = False,
                  is_case_sensitive: bool = True) -> "QueryBuilder":
        return self.add_having_condition(operator, parameter, value, custom, "OR", is_case_sensitive)

    def limit(self, limit: int) -> "QueryBuilder":
        self.limit_value = limit
        return self

    def offset(self, offset: int) -> "QueryBuilder":
        self.offset_value = offset
        return self

    def select(self, custom_select: str) -> "QueryBuilder":
        self.custom_select = custom_select
        return self

    def build_query_string(self) -> str:
        """
        Construct the full SQL query string.
        """
        base_query = f"SELECT {self.custom_select} FROM {self.table_name} {self.alias}"
        where_clause = f" WHERE {' '.join(self.conditions)}" if self.conditions else ""
        group_by_clause = f" GROUP BY {', '.join(self.group_by_fields)}" if self.group_by_fields else ""
        having_clause = f" HAVING {' '.join(self.having_conditions)}" if self.having_conditions else ""
        order_by_clause = f" ORDER BY {', '.join(self.order_by_clauses)}" if self.order_by_clauses else ""
        limit_clause = f" LIMIT {self.limit_value}" if self.limit_value is not None else ""
        offset_clause = f" OFFSET {self.offset_value}" if self.offset_value is not None else ""
        return base_query + self.join_clause + where_clause + group_by_clause + having_clause + order_by_clause + limit_clause + offset_clause

    # For async execution
    # We'll cache results with the synchronous cache, but be careful with concurrency.
    # Locking will void any attempt of parallelization

    async def execute(self) -> List[Dict[str, Any]]:
        """
        Execute the query asynchronously using psycopg3, returning a list of dicts.
        Uses a global LRUCache if available.
        """
        query_string = self.build_query_string()

        # Build a cache key from the SQL + parameters
        cache_key = (query_string, frozenset(self.parameters.items()))
        if cache_key in self.global_cache:
            self.logger.debug("Returning cached result for query: %s", query_string)
            return self.global_cache[cache_key]

        self.logger.debug("Executing query:\n%s", query_string)
        self.logger.debug("Parameters: %s", self.parameters)

        # Run the query
        async with self.pool.connection() as conn:
            # Convert :param style to %(param)s style
            converted_query, converted_params = self._convert_params_for_psycopg(query_string, self.parameters)

            # Execute the query
            async with conn.cursor() as cursor:
                await cursor.execute(converted_query, converted_params)

                # Fetch all rows and convert to list of dicts
                rows = await cursor.fetchall()

        # Cache the result
        result_set = [dict(row) for row in rows]
        self.global_cache[cache_key] = result_set
        return result_set

    def _convert_params_for_psycopg(self, query: str, parameters: Dict[str, Any]) -> (str, Dict[str, Any]):
        """
        Convert :param style to psycopg3 named parameter style: %(param)s
        E.g., "WHERE name=:p1" -> "WHERE name=%(p1)s"
        """
        new_query = query
        new_params = {}
        for named_param, val in parameters.items():
            # Replace :named_param with %(named_param)s
            placeholder = f":{named_param}"
            new_placeholder = f"%({named_param})s"
            new_query = new_query.replace(placeholder, new_placeholder)
            new_params[named_param] = val
        return new_query, new_params
