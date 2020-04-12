"""

"""

import csv
import sqlite3
from src.utils import start_logging

LOGGER = start_logging(level='INFO', log_name=__name__)
KEY_SET = set()
DATA = '../data/FridgeGeo150.csv'
# db = sqlite3.connect(':memory:')


def duplicate_key(data):
    """

    Args:
        data:

    Returns:

    """
    if data['Loc_key'] not in KEY_SET:
        KEY_SET.add(data['Loc_key'])
        return False
    else:
        LOGGER.info('Duplicate key found!')
        return True


def main():
    with open(DATA) as file:
        reader = csv.DictReader(file, lineterminator='\n', delimiter=',')
        for row in reader:
            if not duplicate_key(row):
                pass
            else:
                # TODO: Examine bad data
                continue


if __name__ == '__main__':
    main()
