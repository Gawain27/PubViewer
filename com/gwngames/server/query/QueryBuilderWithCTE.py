from typing import List

from com.gwngames.server.query.QueryBuilder import QueryBuilder


class RecursiveQueryBuilder(QueryBuilder):
    def __init__(self, session, entity_class, alias):
        super().__init__(session, entity_class, alias)
        self.cte_clauses: List[str] = []  # Store CTE definitions
        self.cte_name = None

    def add_cte(self, name: str, definition: str):
        """
        Add a CTE (Common Table Expression) to the query.

        :param name: The name of the CTE.
        :param definition: The SQL definition of the CTE.
        """
        self.cte_clauses.append(f"{name} AS ({definition})")
        self.cte_name = name

    def build_query_string(self) -> str:
        """
        Override the base class to include CTEs in the query.

        :return: Full SQL query string with CTEs.
        """
        cte_clause = f"WITH RECURSIVE {', '.join(self.cte_clauses)}" if self.cte_clauses else ""
        base_query = (
            f"SELECT {self.custom_select} FROM {self.entity_class.__tablename__} {self.alias}"
            if self.entity_class is not None
            else f"SELECT {self.custom_select} FROM {self.cte_name} cte"
        )
        where_clause = f" WHERE {' '.join(self.conditions)}" if self.conditions else ""
        order_by_clause = f" ORDER BY {', '.join(self.order_by_clauses)}" if self.order_by_clauses else ""
        group_by_clause = f" GROUP BY {', '.join(self.group_by_fields)}" if self.group_by_fields else ""
        limit_clause = f" LIMIT {self.limit_value}" if self.limit_value is not None else ""
        offset_clause = f" OFFSET {self.offset_value}" if self.offset_value is not None else ""
        having_clause = f" HAVING {' '.join(self.having_conditions)}" if self.having_conditions else ""
        return f"{cte_clause} {base_query}{self.join_clause}{where_clause}{group_by_clause}{having_clause}{order_by_clause}{limit_clause}{offset_clause}"
