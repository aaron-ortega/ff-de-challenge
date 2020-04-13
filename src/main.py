"""

"""

import csv
import sqlite3
from io import BytesIO, StringIO
from src.utils import start_logging
from time import time

LOGGER = start_logging(level='INFO', log_name=__name__)
KEY_SET = set()
DATA = '../data/FridgeGeo150.csv'
BAD_DATA = '../data/bad_data.csv'
# db = sqlite3.connect(':memory:')


def duplicate_key(key, key_set=None):
    """
    Checks if location key is duplicated
    Args:
        key: string key
        key_set: contains the keys that have been checked

    Returns:
        Boolean denoting if key was duplicated
    """
    if key not in key_set:
        key_set.add(key)
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
    """ """
    start = time()
    stream = BytesIO()
    bad_stream = StringIO()
    bad_stream.write('Loc_key,Latitude,Longitude\n')
    with open(DATA) as file:
        reader = csv.DictReader(file, lineterminator='\n', delimiter=',')
        for row in reader:
            if not duplicate_key(row['Loc_key'], key_set=KEY_SET)\
                    and not type_error(row)\
                    and in_range(row):
                pass
            else:
                bad_stream.write(f"{row['Loc_key']},{row['Latitude']},{row['Longitude']}\n")
                continue

            # Write to byte stream (mapbox api expects this format)
            stream.write(f"{row['Latitude']},{row['Longitude']}\n".encode())
        LOGGER.info(f'Transform time is {1e3 * (time() - start):.2f} ms')

    # Write bad rows to file
    with open(BAD_DATA, 'w') as err_data:
        err_data.write(bad_stream.getvalue())
        bad_stream.close()

    # TODO: integrate stream to mapbox


if __name__ == '__main__':
    main()
