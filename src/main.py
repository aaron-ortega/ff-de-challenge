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
        LOGGER.warning(f'Duplicate key found! Key: {key}')
        return True


def type_error(coordinates):
    """
    Check if coordinate values are float type
    Args:
        coordinates: data with information

    Returns:
        None - type is valid float
        True - type is not valid
    """
    try:
        float(coordinates['Latitude'])
        float(coordinates['Longitude'])
    except ValueError:
        LOGGER.warning(f"Bad coordinates: ({coordinates['Latitude']},{coordinates['Longitude']})")
        return True


def in_range(data):
    """
    Checks if coordinates are outside the expected range.
    The range values where determined in profile_data.ipynb and correspond w/Milwaukee
    and the Greater Chicago Metro area.
    Args:
        data: coordinate info

    Returns:
        _range: boolean determining if coordinate is within range
    """
    _range = (44 > float(data['Latitude']) > 41) and (-87 > float(data['Longitude']) > -89)
    return _range


def main():
    with open(DATA) as file:
        reader = csv.DictReader(file, lineterminator='\n', delimiter=',')
        for row in reader:
            if not duplicate_key(row['Loc_key'])\
                    and not type_error(row)\
                    and in_range(row):
                pass
            else:
                # TODO: Examine bad data
                print(row)
                continue


if __name__ == '__main__':
    main()
