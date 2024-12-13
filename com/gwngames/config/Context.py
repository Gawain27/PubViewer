import logging
import os.path
import threading

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import Session, sessionmaker


class Context:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    # Add attributes as needed
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self._drivers = {}
            self.initialized = True
            self.logger = logging.getLogger('Context')
            self._current_dir = None
            self._config = None
            self._session_maker = None
            self._database = None

    def build_path(self, path: str):
        return os.path.join(self.get_current_dir(), path)

    def get_current_dir(self):
        with self._lock:
            return self._current_dir

    def set_current_dir(self, current_dir):
        with self._lock:
            self._current_dir = current_dir
            self.logger.info("Context added: current active directory: " + current_dir)

    def get_config(self):
        with self._lock:
            from com.gwngames.utils.JsonReader import JsonReader
            _dir: JsonReader = self._config
            return _dir

    def set_config(self, config):
        with self._lock:
            self._config = config
            self.logger.info("Context added: Set current config: " + config.file)

    def set_session_maker(self, session_maker: sessionmaker):
        """
        Set the sessionmaker for the context.
        """
        if not isinstance(session_maker, sessionmaker):
            raise ValueError("session_maker must be an instance of sqlalchemy.orm.sessionmaker")
        self._session_maker = session_maker

    def get_session(self) -> Session:
        """
        Return a new SQLAlchemy session.

        :return: A new SQLAlchemy session instance.
        """
        with self._lock:
            if not self._session_maker:
                raise RuntimeError("Session maker has not been initialized. Call set_session_maker first.")

            # Create and return a new session
            return self._session_maker()

    def set_database(self, database: SQLAlchemy):
        with self._lock:
            self._database = database

    def get_database(self) -> SQLAlchemy:
        with self._lock:
            return self._database
