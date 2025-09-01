import logging
from .core import time

logging.time = time


def get_logger(name, time=time) -> logging.Logger:
    # Configure logging
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(name)
