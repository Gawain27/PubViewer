from typing import Dict
from com.gwngames.server.query.QueryBuilder import QueryBuilder
import threading

TABLE_CACHE: Dict[str, QueryBuilder] = {}
CACHE_LOCK = threading.Lock()


def store_query_builder(table_id: str, qb: QueryBuilder) -> None:
    """Store a QueryBuilder in the global cache under the table_id key."""
    with CACHE_LOCK:  # Ensure thread-safe access
        TABLE_CACHE[table_id] = qb

    timer = threading.Timer(86400, remove_query_builder, args=[table_id])
    timer.daemon = True
    timer.start()


def get_query_builder(table_id: str) -> QueryBuilder:
    """Retrieve a QueryBuilder from the global cache by table_id."""
    with CACHE_LOCK:
        return TABLE_CACHE.get(table_id)


def remove_query_builder(table_id: str) -> None:
    """Remove a QueryBuilder from the global cache if no longer needed."""
    with CACHE_LOCK:
        if table_id in TABLE_CACHE:
            del TABLE_CACHE[table_id]

