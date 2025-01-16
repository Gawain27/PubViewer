import hashlib
import logging
import re
from typing import Any, Dict, List, Optional, Union, Tuple

import cachetools
from psycopg_pool import AsyncConnectionPool

from com.gwngames.config.Context import Context


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
            cache_results: bool = True,
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
        self.current_order_cond: str = None
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
        self.cache_results: bool = cache_results
        self.logger = logging.getLogger(self.__class__.__name__)

        # List (or dict) to hold CTE definitions
        self.ctes: List[Dict[str, Any]] = []

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

    def add_nested_conditions(
            self,
            conditions: List[Tuple[str, str, Any, bool]],
            operator_between_conditions: str = "OR",
            condition_type: str = "AND",
            is_having: bool = False
    ) -> "QueryBuilder":
        """
        Add a nested condition to the query, e.g., AND(cond1 OR cond2 OR cond3 ...).
        """
        nested_conditions = []

        for parameter, operator, value, custom in conditions:

            param_name = self._next_param_name(parameter)
            condition = f"{parameter} {operator} :{param_name}" if not custom else value

            nested_conditions.append(condition)

            if not custom:
                self.parameters[param_name] = value

        nested_clause = f"({f' {operator_between_conditions} '.join(nested_conditions)})"

        # Combine nested clause with the main condition type (e.g., AND)
        if is_having:
            if self.having_conditions:
                self.having_conditions.append(f"{condition_type} {nested_clause}")
            else:
                self.having_conditions.append(nested_clause)
        else:
            if self.conditions:
                self.conditions.append(f"{condition_type} {nested_clause}")
            else:
                self.conditions.append(nested_clause)

        return self

    def limit(self, limit: int) -> "QueryBuilder":
        self.limit_value = limit
        return self

    def offset(self, offset: int) -> "QueryBuilder":
        self.offset_value = offset
        return self

    def select(self, custom_select: str) -> "QueryBuilder":
        self.custom_select = custom_select
        return self

    def from_subquery(self, subquery: "QueryBuilder", subquery_alias: str) -> "QueryBuilder":
        """
        Use another QueryBuilder as a subquery in the FROM clause.
        """
        # Build the subquery string.
        subquery_sql = subquery.build_query_string()

        # Convert subquery parameters to psycopg form
        subquery_sql_converted, subquery_params = subquery._convert_params_for_psycopg(subquery_sql)

        # We'll store the fully parenthesized subquery as if it was our table_name.
        self.table_name = f"({subquery_sql_converted})"
        self.alias = subquery_alias

        # Merge subquery parameters into the parent. We can either rename them or assume
        # subquery param names won't collide. Here we'll rename them:
        new_params = {}
        for k, v in subquery_params.items():
            new_key = f"{subquery_alias}_{k}"
            new_params[new_key] = v

        temp_sql = self.table_name
        for old_key, _ in subquery_params.items():
            old_placeholder = f"%({old_key})s"
            new_placeholder = f"%({subquery_alias}_{old_key})s"
            temp_sql = temp_sql.replace(old_placeholder, new_placeholder)

        self.table_name = temp_sql
        self.parameters.update(new_params)

        return self

    def subquery_condition(
            self,
            parameter: str,
            subquery: "QueryBuilder",
            operator: str = "IN",
            condition_type: str = "AND"
    ) -> "QueryBuilder":
        """
        Add a condition using a subquery, e.g.:
            parameter IN ( SELECT ... )
        """
        subquery_sql = subquery.build_query_string()
        subquery_sql_converted, subquery_params = subquery._convert_params_for_psycopg(subquery_sql)

        # Rename subquery parameters to avoid collisions
        new_params = {}
        for k, v in subquery_params.items():
            new_key = f"subq_{k}"
            new_params[new_key] = v

        temp_sql = subquery_sql_converted
        for old_key, _ in subquery_params.items():
            old_placeholder = f"%({old_key})s"
            new_placeholder = f"%(subq_{old_key})s"
            temp_sql = temp_sql.replace(old_placeholder, new_placeholder)

        # Construct the condition string
        condition_str = f"{parameter} {operator} ({temp_sql})"

        if self.conditions:
            self.conditions.append(f"{condition_type} {condition_str}")
        else:
            self.conditions.append(condition_str)

        # Merge subquery parameters
        self.parameters.update(new_params)

        return self

    # ----------------------
    # NEW: CTE support
    # ----------------------

    def with_cte(
            self,
            cte_name: str,
            subquery: Union[str, "QueryBuilder"]
    ) -> "QueryBuilder":
        """
        Add a CTE (Common Table Expression) to the query.

        :param cte_name: The name of the CTE (e.g. "my_cte").
        :param subquery: Either a raw SQL string or another QueryBuilder instance.
        """
        if isinstance(subquery, QueryBuilder):
            # 1. Build the subquery
            subquery_sql = subquery.build_query_string()
            # 2. Convert to psycopg placeholders
            subquery_sql_converted, subquery_params = subquery._convert_params_for_psycopg(subquery_sql)

            # 3. Rename subquery parameters to avoid collisions
            new_params = {}
            for k, v in subquery_params.items():
                new_key = f"{cte_name}_{k}"
                new_params[new_key] = v

            # 4. Replace placeholders in the subquery SQL
            temp_sql = subquery_sql_converted
            for old_key, _ in subquery_params.items():
                old_placeholder = f"%({old_key})s"
                new_placeholder = f"%({cte_name}_{old_key})s"
                temp_sql = temp_sql.replace(old_placeholder, new_placeholder)

            # 5. Store the final CTE definition
            self.ctes.append({
                "cte_name": cte_name,
                "sql": temp_sql
            })

            # 6. Merge the parameters
            self.parameters.update(new_params)

        else:
            # subquery is a raw SQL string
            self.ctes.append({
                "cte_name": cte_name,
                "sql": subquery
            })

        return self

    def build_query_string(self) -> str:
        """
        Construct the full SQL query string, including any CTEs if present.
        """
        # Build the WITH clause if we have CTEs
        with_clause = ""
        if self.ctes:
            cte_statements = []
            for cte_def in self.ctes:
                cte_statements.append(f"{cte_def['cte_name']} AS ( {cte_def['sql']} )")
            with_clause = f"WITH {', '.join(cte_statements)} "

        base_query = f"SELECT {self.custom_select} FROM {self.table_name} {self.alias}"

        where_clause = f" WHERE {' '.join(self.conditions)}" if self.conditions else ""
        group_by_clause = f" GROUP BY {', '.join(self.group_by_fields)}" if self.group_by_fields else ""
        having_clause = f" HAVING {' '.join(self.having_conditions)}" if self.having_conditions else ""
        order_by_clause = f" ORDER BY {', '.join(self.order_by_clauses)}" if self.order_by_clauses else ""
        limit_clause = f" LIMIT {self.limit_value}" if self.limit_value is not None else ""
        offset_clause = f" OFFSET {self.offset_value}" if self.offset_value is not None else ""

        return with_clause + base_query + self.join_clause + where_clause + group_by_clause + having_clause + order_by_clause + limit_clause + offset_clause

    async def execute(self) -> List[Dict[str, Any]]:
        """
        Execute the query asynchronously using psycopg3, returning a list of dicts.
        Uses a global LRUCache if available.
        """
        query_string = self.build_query_string()

        # Build a cache key from the SQL + parameters
        cache_key = (query_string, frozenset(self.parameters.items()))
        if cache_key in self.global_cache:
            return self.global_cache[cache_key]

        async with self.pool.connection() as conn:
            # Convert :param style to %(param)s style
            converted_query, converted_params = self._convert_params_for_psycopg(query_string)

            async with conn.cursor() as cursor:
                logging.info(f"Executing query: {converted_query}")
                logging.info(f"Params: {converted_params}")
                await cursor.execute(converted_query, converted_params)
                rows = await cursor.fetchall()

        result_set = [dict(row) for row in rows]
        if self.cache_results is True:
            self.global_cache[cache_key] = result_set
        return result_set

    def clone(self, no_offset=False, no_limit=False) -> "QueryBuilder":
        """
        Create a deep copy of the current QueryBuilder instance, creating a new session for it.
        """
        from copy import deepcopy

        cloned_instance = QueryBuilder(
            pool=Context().get_pool(),
            table_name=self.table_name,
            alias=self.alias,
        )

        # Deep copy various attributes
        cloned_instance.conditions = deepcopy(self.conditions)
        cloned_instance.parameters = deepcopy(self.parameters)
        cloned_instance.order_by_clauses = deepcopy(self.order_by_clauses)
        cloned_instance.join_clause = self.join_clause
        cloned_instance.custom_select = self.custom_select
        cloned_instance.group_by_fields = deepcopy(self.group_by_fields)
        cloned_instance.having_conditions = deepcopy(self.having_conditions)
        cloned_instance.param_counter = self.param_counter

        # Copy CTEs
        cloned_instance.ctes = deepcopy(self.ctes)

        cloned_instance.logger = self.logger

        if not no_limit:
            cloned_instance.limit_value = self.limit_value
        if not no_offset:
            cloned_instance.offset_value = self.offset_value

        return cloned_instance

    def _convert_params_for_psycopg(self, query: str) -> (str, Dict[str, Any]):
        """
        Convert :param style (e.g., :p1) to psycopg3 named parameter style (e.g., %(p1)s)
        """
        new_query = query
        new_params = {}
        logging.info(f"Parameters: {self.parameters}")

        pattern = re.compile(r':([A-Za-z0-9_]+)')

        def replace_param(m):
            param_name = m.group(1)
            new_params[param_name] = self.parameters[param_name]
            return f"%({param_name})s"

        new_query = pattern.sub(replace_param, new_query)
        return new_query, new_params


