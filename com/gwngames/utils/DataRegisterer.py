import threading


class DataRegisterer:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(DataRegisterer, cls).__new__(cls, *args, **kwargs)
                    cls._instance._data = set()
        return cls._instance

    def add(self, item):
        with self._lock:
            if item not in self._instance._data:
                self._instance._data.add(item)
                return True
            return False

    def remove(self, item):
        with self._lock:
            self._instance._data.remove(item)

    def __contains__(self, item):
        with self._lock:
            return item in self._instance._data

    def items(self):
        with self._lock:
            return set(self._instance._data)

    def clear(self):
        with self._lock:
            self._instance._data.clear()

    def update(self, *items):
        with self._lock:
            added = False
            for item in items:
                if item not in self._instance._data:
                    self._instance._data.add(item)
                    added = True
            return added

    def __len__(self):
        with self._lock:
            return len(self._instance._data)

    def __str__(self):
        with self._lock:
            return str(self._instance._data)

    def __repr__(self):
        with self._lock:
            return repr(self._instance._data)

    def add_all(self, items):
            added = False
            for item in items:
                if item not in self._instance._data:
                    self._instance._data.add(item)
                    added = True
            return added
