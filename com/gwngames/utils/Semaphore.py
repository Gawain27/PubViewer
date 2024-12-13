import threading


class SingletonSemaphore:
    _instances = {}

    def __new__(cls, name: str, initial_count: int = 1):
        if name not in cls._instances:
            instance = super().__new__(cls)
            instance._is_initialized = False
            cls._instances[name] = instance
        return cls._instances[name]

    def __init__(self, name: str, initial_count: int = 1):
        if not self._is_initialized:
            self.name = name
            self.semaphore = threading.Semaphore(initial_count)
            self._is_initialized = True

    def acquire(self):
        self.semaphore.acquire()

    def release(self):
        self.semaphore.release()

    def __repr__(self):
        return f"<SingletonSemaphore(name={self.name})>"
