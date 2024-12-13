import threading


class LoadState:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(LoadState, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._value = 0
            self._keepdown = False
            self._keepdown_lock = threading.Lock()
            self._value_lock = threading.Lock()
            self._initialized = True

    @property
    def load_perc(self):
        with self._value_lock:
            return self._value

    @load_perc.setter
    def load_perc(self, new_value):
        if not (0 <= new_value <= 100):
            raise ValueError("Value must be between 0 and 100")
        with self._value_lock:
            self._value = new_value

    @property
    def keepdown(self):
        with self._keepdown_lock:
            return self._keepdown

    @keepdown.setter
    def keepdown(self, new_value):
        if not (new_value == 0 or new_value == 1):
            raise ValueError("Value must be either 0 or 1")
        with self._keepdown_lock:
            self._keepdown = new_value
