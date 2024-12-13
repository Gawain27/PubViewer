import logging
import threading
from datetime import datetime

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class RequestState:
    _instance = None  # todo make request per interface, or per msg type
    _lock = threading.Lock()

    def __init__(self):
        with self._lock:
            if self.__initialized:
                return
            self.__initialized = True
            self.stored_date = datetime.now()
            self.max_concurrent_requests = JsonReader(JsonReader.CONFIG_FILE_NAME).get_value(
                ConfigConstants.MAX_IFACE_REQUESTS)
            self.active_count = 0
            self.groups_active = {}
            self.logger = logging.getLogger("RequestState")
            self.condition = threading.Condition()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:  # Double-checked locking
                    cls._instance = super(RequestState, cls).__new__(cls, *args, **kwargs)
                    cls._instance.__initialized = False  # Ensure initialization flag is set before init
        return cls._instance

    def update_last_sent(self, msg: AbstractMessage):
        start_time = datetime.now()
        with self.condition:
            while True:
                if self.active_count < self.max_concurrent_requests and self.groups_active.pop(msg.get_group_key(), False) is False:
                    self.active_count += 1
                    self.groups_active[msg.get_group_key()] = True
                    break
                else:
                    self.groups_active[msg.get_group_key()] = True
                    self.condition.wait()
        self.logger.info(f"Serving next request {msg.get_group_key()} - {msg.message_id} after waited time {datetime.now() - start_time} - Total: {self.active_count}")

    def notify_reschedule(self, msg: AbstractMessage):
        with self.condition:
            while True:
                if self.active_count < self.max_concurrent_requests and self.groups_active.pop(msg.get_group_key(), False) is False:
                    self.logger.info(f"Rescheduled message {msg.message_id}")
                    self.active_count += 1
                    self.groups_active[msg.get_group_key()] = True
                    break
                else:
                    self.groups_active[msg.get_group_key()] = True
                    self.condition.wait()

    def notify_update(self, msg: AbstractMessage):
        with self.condition:
            if self.active_count > 0:
                self.active_count -= 1
                if msg.get_group_key() is not None:
                    self.groups_active[msg.get_group_key()] = False
                self.condition.notify_all()
