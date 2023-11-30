import functools
import logging
import time
from typing import Callable, Any


def log_function(func: Callable[[[Any], {str, Any}], Any]):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"Running {func.__name__}")
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logging.info(f"Finished Running {func.__name__} took {start - end} seconds")
        return result
    return wrapper
