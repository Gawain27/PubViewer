import os


class FileUtils(object):
    @staticmethod
    def find(query):
        current_directory = os.getcwd()
        files = os.listdir(current_directory)

        # Use a generator expression to find the first file that contains the query string
        matching_file = next((file for file in files if query in file), None)

        return matching_file  # Returns None if no matching file found
