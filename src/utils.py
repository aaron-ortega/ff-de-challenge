"""
Utils used by main.py
"""

from os.path import join, exists
import configparser
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


def get_credentials(dir_='../config'):
    """
    Read secret.ini file
    Args:
        dir_: directory where credentials are found

    Returns:
        ConfigParser object
    """

    config = configparser.ConfigParser()
    secret_path = join(dir_, 'secrets.ini')
    if exists(secret_path):
        return config.read(secret_path)
    else:
        raise Exception('Config file(s) not in config path!')
