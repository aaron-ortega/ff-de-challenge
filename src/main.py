"""
This script does the following:
    1. Ingests coordinates data
    2. Separates valid and invalid data
    3. Saves (to disk) invalid data for review
    4. Transforms valid data from csv to Geojson
    5. Integrates geojson to Mapbox

How to run:
    python3 ./src/main.py --tile_name <NAME>

Eg.
    python3 ./src/main.py --tile_name chicago_milwaukee_data
"""

import argparse
import csv
import json
from collections import defaultdict
from io import BytesIO, StringIO
from mapbox import Uploader
from src.utils import start_logging, get_credentials
from time import time

LOGGER = start_logging(level='INFO', log_name=__name__)
KEY_SET = set()
CONFIG = get_credentials()
TOKEN = CONFIG.get('mapbox-api', 'token')
DATA = '../data/FridgeGeo150.csv'
BAD_DATA = '../data/bad_data.csv'


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


def main(tile_name):
    """
    Capture valid data, pipe it to a BytesIO stream, and store it to our Mapbox account.
    Capture invalid data, pipe it to a StringIO stream, and store locally for examination.
    """
    start = time()
    clean_data = defaultdict(list)
    bad_stream = StringIO()
    bad_stream.write('Loc_key,Latitude,Longitude\n')  # Add header
    with open(DATA) as file:
        reader = csv.DictReader(file, lineterminator='\n', delimiter=',')
        for row in reader:
            if not duplicate_key(row['Loc_key'], key_set=KEY_SET)\
                    and not type_error(row)\
                    and in_range(row):
                clean_data['coordinates'].append([
                    float(row['Longitude']),
                    float(row['Latitude'])]
                )
            else:
                bad_stream.write(f"{row['Loc_key']},{row['Latitude']},{row['Longitude']}\n")
                continue

        # Add meta to conform to Geojson specifications
        clean_data['type'] = 'MultiPoint'

    # Write to byte stream (mapbox api expects this format)
    stream = BytesIO()
    stream.write(json.dumps(clean_data).encode())
    stream.seek(0)

    LOGGER.info(f'Transform time is {1e3 * (time() - start):.2f} ms')
    LOGGER.info(f'Number of coordinate points: {len(clean_data["coordinates"])}')

    # Release resources
    del clean_data

    # Write bad rows to file
    with open(BAD_DATA, 'w') as err_data:
        err_data.write(bad_stream.getvalue())
        bad_stream.close()

    # Connect to mapbox API and upload stream
    service = Uploader(access_token=TOKEN)
    upload_response = service.upload(stream, tile_name)

    if upload_response.status_code == 201:
        LOGGER.info(f'Status: 201; Mapbox received stream!')
        stream.close()
    else:
        LOGGER.info(f'Other code')
        print(f'{upload_response.status_code}, {upload_response.json()}')
        # TODO: add handling of other status codes (400, 500, etc.)


if __name__ == '__main__':
    # Pass name of tileset (how Mapbox will name the uploaded data)
    parser = argparse.ArgumentParser()
    parser.add_argument('--tile_name', required=True, help='reference name of uploaded data')
    args = parser.parse_args()
    main(args.tile_name)
