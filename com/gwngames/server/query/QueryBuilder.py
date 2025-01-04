import hashlib
import logging
from typing import Any, Dict, List, Optional, Union, Tuple

import cachetools
from psycopg_pool import AsyncConnectionPool

from com.gwngames.config.Context import Context


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
        :param join_type: The type of JOIN clause.
        :param join_alias: The alias of the JOIN clause.
        :param on_condition: The condition to apply on the JOIN clause.
        :param this_field: The field to use for the JOIN clause.
        :param other_field: The field to use for the JOIN clause, on joined table
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

        :param conditions: A list of conditions, where each condition is a tuple containing:
                           (parameter, operator, value, custom, is_case_sensitive).
                           `custom` indicates if the condition is a raw SQL condition.
        :param operator_between_conditions: The operator between the nested conditions, e.g., OR.
        :param condition_type: The operator for the overall condition, e.g., AND.
        :param is_having: Whether the condition is an HAVING condition.
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

        # Because we need to embed them directly, replace the 'FROM' part with parentheses.
        # We'll store the fully parenthesized subquery as if it was our table_name.
        # Example: self.table_name = ( SELECT ... ) -> with the subquery alias appended
        self.table_name = f"({subquery_sql_converted})"
        self.alias = subquery_alias

        # Merge subquery parameters into the parent. We can either rename them or assume
        # subquery param names won't collide. A safer approach is to rename them:
        new_params = {}
        for k, v in subquery_params.items():
            # For instance, prefix the key with the alias or anything you want.
            # e.g., "sub_{alias}_{param}".
            new_key = f"{subquery_alias}_{k}"
            new_params[new_key] = v

        # Now replace the placeholders in the subquery SQL to reference the new prefix:
        # We must do this carefully: replace "%(oldparam)s" with "%(newparam)s"
        # so that the parent's final "table_name" contains the correct placeholders.

        temp_sql = self.table_name
        for old_key, _ in subquery_params.items():
            old_placeholder = f"%({old_key})s"
            new_placeholder = f"%({subquery_alias}_{old_key})s"
            temp_sql = temp_sql.replace(old_placeholder, new_placeholder)

        # Now assign the fixed-up string back to our "table_name".
        # This is the final parenthesized subquery with updated param placeholders.
        self.table_name = temp_sql

        # Merge these new parameters into the parent's parameters dict
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
        # 1. Build the subquery SQL
        subquery_sql = subquery.build_query_string()
        subquery_sql_converted, subquery_params = subquery._convert_params_for_psycopg(subquery_sql)

        # 2. Rename subquery parameters to avoid collisions
        #    We'll use a simple prefix "subq_" here, but you can get more fancy if needed.
        new_params = {}
        for k, v in subquery_params.items():
            new_key = f"subq_{k}"
            new_params[new_key] = v

        # 3. Replace placeholders in the subquery SQL with the new param names
        temp_sql = subquery_sql_converted
        for old_key, _ in subquery_params.items():
            old_placeholder = f"%({old_key})s"
            new_placeholder = f"%(subq_{old_key})s"
            temp_sql = temp_sql.replace(old_placeholder, new_placeholder)

        # 4. Construct the condition string, e.g. "parameter IN (<subquery>)"
        condition_str = f"{parameter} {operator} ({temp_sql})"

        # 5. Add condition to the parent's conditions
        if self.conditions:
            self.conditions.append(f"{condition_type} {condition_str}")
        else:
            self.conditions.append(condition_str)

        # 6. Merge subquery parameters into parent's parameters
        self.parameters.update(new_params)

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
        self.global_cache[cache_key] = result_set
        return result_set

    def clone(self, no_offset = False, no_limit = False) -> "QueryBuilder":
        """
        Create a deep copy of the current QueryBuilder instance, creating a new session for it.
        """
        from copy import deepcopy

        # Create a new instance without the session
        cloned_instance = QueryBuilder(
            pool=Context().get_pool(),  # Derive a new session
            table_name=self.table_name,
            alias=self.alias,
        )

        # Deep copy attributes to ensure no shared state
        cloned_instance.conditions = deepcopy(self.conditions)
        cloned_instance.parameters = deepcopy(self.parameters)
        cloned_instance.order_by_clauses = deepcopy(self.order_by_clauses)
        cloned_instance.join_clause = self.join_clause  # Strings are immutable, no need to deepcopy
        cloned_instance.custom_select = self.custom_select
        cloned_instance.group_by_fields = deepcopy(self.group_by_fields)
        cloned_instance.having_conditions = deepcopy(self.having_conditions)
        cloned_instance.param_counter = self.param_counter

        cloned_instance.logger = self.logger

        if no_limit is False:
            cloned_instance.limit_value = self.limit_value
        if no_offset is False:
            cloned_instance.offset_value = self.offset_value

        return cloned_instance

    def _convert_params_for_psycopg(self, query: str) -> (str, Dict[str, Any]):
        """
        Convert :param style to psycopg3 named parameter style: %(param)s
        E.g., "WHERE name=:p1" -> "WHERE name=%(p1)s"
        """
        new_query = query
        new_params = {}
        logging.info(f"Parameters: {self.parameters}")
        for named_param, val in self.parameters.items():
            # Replace :named_param with %(named_param)s
            placeholder = f":{named_param}"
            new_placeholder = f"%({named_param})s"
            new_query = new_query.replace(placeholder, new_placeholder)
            new_params[named_param] = val
            logging.info(f"Converted {new_query} to {new_params}")
        return new_query, new_params

