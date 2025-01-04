import logging
import os.path
import threading
from typing import Optional

from psycopg_pool import AsyncConnectionPool

class Context:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.logger = logging.getLogger('Context')
            self._current_dir: Optional[str] = None
            self._config = None

            # Instead of session_maker, we hold a reference to the psycopg async pool
            self._pool: Optional[AsyncConnectionPool] = None

    def build_path(self, path: str):
        with self._lock:
            return os.path.join(self.get_current_dir(), path)

    def get_current_dir(self):
        with self._lock:
            return self._current_dir

    def set_current_dir(self, current_dir):
        with self._lock:
            self._current_dir = current_dir
            self.logger.info("Context added: current active directory: " + current_dir)

    from com.gwngames.utils.JsonReader import JsonReader
    def get_config(self) -> JsonReader:
        with self._lock:
            return self._config

    def set_config(self, config):
        with self._lock:
            self._config = config
            self.logger.info("Context added: Set current config: " + config.file)

    def set_pool(self, pool: AsyncConnectionPool):
        """
        Assign an async psycopg connection pool to the context.
        """
        if not isinstance(pool, AsyncConnectionPool):
            raise ValueError("pool must be an instance of psycopg.AsyncConnectionPool")
        self._pool = pool
        self.logger.info("Context: AsyncConnectionPool set.")

    def get_pool(self) -> AsyncConnectionPool:
        if not self._pool:
            raise RuntimeError("Pool has not been initialized. Call set_pool first.")
        return self._pool
