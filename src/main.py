"""

"""

import csv
import sqlite3
from src.utils import start_logging

LOGGER = start_logging(level='INFO', log_name=__name__)
KEY_SET = set()
DATA = '../data/FridgeGeo150.csv'
# db = sqlite3.connect(':memory:')


def duplicate_key(key):
    """
    Checks if location key is duplicated
    Args:
        key: string key

    Returns:
        Boolean denoting if key was duplicated
    """
    if key not in KEY_SET:
        KEY_SET.add(key)
        return False
    else:
        LOGGER.warning('Duplicate key found!')
        return True


def type_error(coordinates):
    """

    Args:
        coordinates:

    Returns:

    """
    try:
        float(coordinates['Latitude'])
        float(coordinates['Longitude'])
    except ValueError:
        LOGGER.warning(f"Bad coordinates: ({coordinates['Latitude']},{coordinates['Longitude']})")
        return True


def main():
    with open(DATA) as file:
        reader = csv.DictReader(file, lineterminator='\n', delimiter=',')
        for row in reader:
            if not duplicate_key(row['Loc_key'])\
                    and not type_error(row):
                pass
            else:
                # TODO: Examine bad data
                continue


if __name__ == '__main__':
    main()
