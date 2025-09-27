import functools
import logging
from logger_factory import get_logger

logger = get_logger("pipeline")
logger.propagate = False

def safe_run(func):
    """
    Decorator to wrap a function with logging and error catching.
    Logs the function start, success, and failure.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Starting: {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"Success: {func.__name__}")
            return result
        except Exception as e:
            logger.exception(f"Error in {func.__name__}: {e}")
            # Reraise to let upstream code decide what to do
            raise
    return wrapper
