from typing import List


class StringUtils:
    class SemicolonFoundException(Exception):
        pass

    @staticmethod
    def process_string(input_string: str) -> List[str]:
        if ';' in input_string:
            raise StringUtils.SemicolonFoundException("Input string contains a semicolon")
        else:
            # Split the string by commas
            result = input_string.split(',')
            return result

    @staticmethod
    def sanitize_string(input_string):
        trimmed_string = input_string.strip()
        invalid_chars = '<>:"/\\|?*'
        sanitized_string = ''.join('' if c in invalid_chars else c for c in trimmed_string)

        return sanitized_string
