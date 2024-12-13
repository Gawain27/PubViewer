import logging
import random
import time


class ThreadUtils:

    @staticmethod
    def random_sleep(min_seconds: float, max_seconds: float, logger: logging.Logger, object_for: str):
        """
        Sleep for a random number of seconds between min_seconds and max_seconds,
        with an additional delta that either adds or subtracts from the sleep time.
        """
        # Use a normal distribution to generate randomness closer to uniformity
        mid_point = (min_seconds + max_seconds) / 2
        range_seconds = (max_seconds - min_seconds) / 2

        sleep_time = random.gauss(mid_point, range_seconds / 3)
        sleep_time = max(min_seconds, min(max_seconds, sleep_time))

        # Generate a delta between 0 and min_seconds
        delta = random.uniform(0, min_seconds)
        if random.choice([True, False]) is False:
            delta = -delta

        sleep_time += delta
        sleep_time = max(min_seconds, sleep_time)

        logger.info(f"Waiting {sleep_time:.2f} seconds for {object_for} (delta: {delta:.2f})...")
        time.sleep(sleep_time)
