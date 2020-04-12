"""
Utils used by main.py
"""

import logging


def start_logging(level='INFO', log_name=None):
    """
    Instantiate a logger
    Args:
        level:
        log_name:

    Returns:
        Logger object
    """

    logger = logging.getLogger(log_name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    logger_format = "%(asctime)s %(levelname)s :: %(message)s"
    formatter = logging.Formatter(logger_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
