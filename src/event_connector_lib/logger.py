import logging
import sys

LOG_FORMAT = "%(asctime)s [%(name)s] [%(process)d] %(levelname)s: %(message)s"


def setup_logging() -> logging.Logger:
    formatter = logging.Formatter(LOG_FORMAT)
    logger = logging.getLogger("EventConnectorLib")

    if not logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logger


logger = setup_logging()
