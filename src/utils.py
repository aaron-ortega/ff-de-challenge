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
    public = join(dir_, 'config.ini')
    secret = join(dir_, 'secrets.ini')
    if exists(public) and exists(secret):
        config.read([public, secret])
        return config
    else:
        raise Exception('Config file(s) not in config path!')
