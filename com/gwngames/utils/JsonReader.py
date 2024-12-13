import json
import logging
import os
import threading
from typing import Final, Any

from com.gwngames.config.Context import Context


class JsonReader:
    """
    A class for reading and getting values from a json file.

    NOTE: If you use JsonReader.BASE_DIR, the directory of the active script file will be cached


    :param file: The path to the json file.
    :param directory: The local directory where the file is located.
    """
    DEV_NULL: Final = '/dev/null'
    CONFIG_FILE_NAME: Final = 'config.json'
    MESSAGE_STAT_FILE_NAME: Final = 'message_stats.json'

    ctx = Context()
    _locks = {}  # Class-level dictionary to hold locks for each file
    _directories: dict = {}  # Class-level dictionary to cache directories upon first use

    def __init__(self, file: str, directory: str = None, parent: str = None):
        self.logger = logging.getLogger("file_" + file) if parent is None else logging.getLogger(parent + "_" + file)
        self.logger.setLevel(logging.DEBUG)  # Set the logging level to DEBUG

        if directory is None:
            directory = JsonReader.ctx.get_current_dir()
        if directory != JsonReader.ctx.get_current_dir():
            directory = JsonReader.ctx.build_path(directory)
        self.directory = directory
        self.data: dict = {}

        if file == JsonReader.DEV_NULL:
            # void call
            return

        self.file = os.path.join(directory, file)

        # Step 1: Check if directory is cached
        existing_dir = JsonReader._directories.get(self.directory)
        if existing_dir is not None:
            self.file = os.path.join(existing_dir, self.file)
            self.directory = existing_dir
        else:
            JsonReader._directories[self.directory] = self.directory
            self.logger.info("Cached directory %s", self.directory)

        # Step 2: Initialize lock for this file if not already present
        if self.file not in JsonReader._locks:
            JsonReader._locks[self.file] = threading.Lock()
        self.lock = JsonReader._locks[self.file]

        # Step 3: Create the directory if it doesn't exist
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            self.logger.info(f"Created directory '{self.directory}'.")

        # Step 4: create or open file
        self.load_file()

    def load_file(self, create: bool = False):
        """
        Load the data from the specified file.

        :return: None
        """
        with self.lock:
            try:
                file_size = os.path.getsize(self.file)

                if file_size == 0:
                    self.data = {}
                else:
                    with open(self.file, 'r') as f:
                        self.data = json.load(f)
            except FileNotFoundError as e:
                if create:
                    with open(self.file, 'w') as f:
                        self.data = {}
                        json.dump(self.data, f)
                        self.save_changes()
                        self.logger.info(f"Created new file '{self.file}' and initialized with empty data.")
            except json.JSONDecodeError:
                self.logger.error(f"Error: Invalid JSON format in file '{self.file}'.")

    def get_value(self, key: str) -> Any:
        """
        Retrieve the value for the specified key from the configuration data.

        :param key: The key to look up.
        :return: The value corresponding to the key. None if key not found.
        """
        if self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")
            return self.data.get(key, None)

    def set_value(self, key: str, value):
        """
        Set the value for the specified key in the configuration data.

        :param key: The key to set the value for.
        :param value: The value to set.
        :return: None
        """
        if self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")

            self.data[key] = value
            self.save_changes()

    def save_changes(self):
        """
        Save the updated configuration data back to the file.

        :return: None
        """
        if self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")

            try:
                with open(self.file, 'w') as f:
                    json.dump(self.data, f, indent=4)
            except IOError as e:
                self.logger.error(f"Error saving changes to '{self.file}': {e}")
        else:
            self.logger.error(f"No changes saved to '{self.file}' successfully.")

    def clear(self, key: str):
        """
        Clear the specified key from the configuration data.

        :param key: The key to delete.
        :return: None
        """
        with self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")

            if key in self.data:
                del self.data[key]
                self.logger.info(f"Cleared key '{key}' from the data.")
                self.save_changes()
            else:
                self.logger.warning(f"Key '{key}' not found in the data.")

    def delete_file(self):
        """
        Delete the entire file associated with this instance of FileReader.

        :return: None
        """
        with self.lock:
            if os.path.exists(self.file):
                with open(self.file, 'r+b') as f:
                    length = f.tell()
                    for _ in range(3):
                        f.seek(0)
                        f.write(os.urandom(length))
                os.remove(self.file)
                self.logger.info(f"{self.file} has been securely deleted.")
            else:
                self.logger.warning(f"{self.file} does not exist.")

    def set_and_save(self, key: str, value):
        """
        Sets the value of a given key and saves the changes.

        :param key: The key to set the value for.
        :param value: The value to set.
        :return: None
        """
        with self.lock:
            self.set_value(key, value)
            self.save_changes()

    def increment(self, key: str):
        with self.lock:
            prev = self.get_value(key)
            if prev is None:
                prev = 0
            self.set_value(key, prev + 1)
            self.save_changes()

    def dump_and_save(self, dump: Any):
        """
            Save data.

            :param self:
            :param dump: Optional data to append.
            :return: None
            """
        with self.lock:
            with open(self.file, 'a') as f:
                self.data = dump
            self.save_changes()

    def is_empty(self):
        if self.data is None:
            self.data = {}
            return True
        return self.data.items().__len__() == 0

    def is_outdated(self):
        # todo implement outdate logic for files
        return False
