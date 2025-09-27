import logging
import sys

def get_logger(name: str = None, level=logging.INFO) -> logging.Logger:
    """
    Returns a configured logger instance.
    """
    logger = logging.getLogger(name if name else __name__)
    logger.setLevel(level)

    # Avoid adding multiple handlers if logger already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
