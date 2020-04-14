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
import requests
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


def request_isochrone(coordinates):
    """
    Get walkable area of a given point using Mapbox API
    Args:
        coordinates: data point

    Returns:
        Geojson with 5 and 10 min walkable area
    """
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    params = {
        'contours_minutes': '5,10',
        'contours_colors': 'b53c2c,2c47dd',
        'polygons': 'true',
        'access_token': TOKEN
    }
    profile = 'mapbox/walking'
    url = 'https://api.mapbox.com/isochrone/v1/{}/{},{}'.format(profile, *coordinates)
    res = requests.get(url, headers=headers, params=params)
    return res.json()


def upload_to_mapbox():
    return 0


def main(tile_name):
    """
    Capture valid data, pipe it to a BytesIO stream, and store it to our Mapbox account.
    Capture invalid data, pipe it to a StringIO stream, and store locally for examination.
    """
    start = time()
    point_data = defaultdict(list)
    bad_stream = StringIO()
    bad_stream.write('Loc_key,Latitude,Longitude\n')  # Add header
    with open(DATA) as file:
        reader = csv.DictReader(file, lineterminator='\n', delimiter=',')
        for row in reader:
            if not duplicate_key(row['Loc_key'], key_set=KEY_SET)\
                    and not type_error(row)\
                    and in_range(row):
                point_data['coordinates'].append([
                    float(row['Longitude']),
                    float(row['Latitude'])]
                )
            else:
                bad_stream.write(f"{row['Loc_key']},{row['Latitude']},{row['Longitude']}\n")
                continue

        # Add meta to conform to Geojson specifications
        point_data['type'] = 'MultiPoint'

    # Collect all area reachable by foot (within 5 & 10 min)
    # for every coordinate
    isochrone_data = defaultdict(list)
    for point in point_data['coordinates']:
        result = request_isochrone(point)
        isochrone_data['features'].append(result['features'][0])  # 5 min
        isochrone_data['features'].append(result['features'][1])  # 10 min

    # Add meta to conform to Geojson specifications
    isochrone_data['type'] = 'FeatureCollection'

    # Write to byte stream (mapbox api expects this format)
    stream = BytesIO()
    stream.write(json.dumps(point_data).encode())
    stream.seek(0)

    LOGGER.info(f'Transform time is {1e3 * (time() - start):.2f} ms')
    LOGGER.info(f'Number of coordinate points: {len(point_data["coordinates"])}')

    # Release resources
    del point_data

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

    try:
        main(args.tile_name)

    except Exception:
        # TODO: add Exception handling
        # TODO: add notification if job fails/succeeds
        pass
