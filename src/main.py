"""
This script does the following:
    1. Ingests coordinates data
    2. Separates valid and invalid data
    3. Saves (to ./data) invalid data for review
    4. Transforms valid data from csv to Geojson
    5. Integrates geojson to Mapbox
    6. Calculates number of fridge overlap
    7. Saves (to ./data) valid data w/overlap count

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
from src.compute import number_of_overlaps
from src.utils import start_logging, get_credentials
from time import time, sleep

LOGGER = start_logging(level='INFO', log_name=__name__)
KEY_SET = set()
CONFIG = get_credentials()
TOKEN = CONFIG.get('mapbox-api', 'token')
DATA = '../data/FridgeGeo150.csv'
BAD_DATA = '../data/bad_data.csv'
CLEAN_DATA = '../data/cleaned_data.json'


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


def unique(row, unique_data):
    """
    Checks if the coordinate has already been added to the clean data
    Args:
        row:
        unique_data:

    Returns:
        True - if data is new (unique)
        False - if data already present
    """
    row = [float(row['Longitude']), float(row['Latitude'])]
    if row not in unique_data:
        return True
    return False


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


def upload_to_mapbox(data, type_=''):
    """
    Upload data to mapbox
    Args:
        data: data to be uploaded
        type_: string detailing data (points, polygons, etc.)
    """
    stream = BytesIO()
    stream.write(json.dumps(data).encode())
    stream.seek(0)

    start = time()
    # Connect to mapbox API and upload stream
    service = Uploader(access_token=TOKEN)
    upload_response = service.upload(stream, type_)

    if upload_response.status_code == 201:
        LOGGER.info(f'Status: 201; Mapbox received {type_} stream!')
        stream.close()
    else:
        # TODO: add handling of other status codes (400, 500, etc.)
        LOGGER.info(f'Other code')
        LOGGER.warning(f'{upload_response.status_code}, {upload_response.json()}')

    # TODO: refactor code this can be done better
    # Wait until data is fully uploaded
    upload_id = upload_response.json()['id']
    while True:
        status_resp = service.status(upload_id).json()
        if status_resp['complete']:
            break
        sleep(3)
    LOGGER.info(f'Completed Upload: {type_}')
    LOGGER.info(f'Time: {time() - start:.2f} s')


def main(tile_name):
    """
    Capture valid data, pipe it to a BytesIO stream, and store it to our Mapbox account.
    Capture invalid data, pipe it to a StringIO stream, and store locally for examination.
    """
    start = time()
    key_data = []
    point_data = defaultdict(list)
    bad_stream = StringIO()
    bad_stream.write('Loc_key,Latitude,Longitude\n')  # Add header
    with open(DATA) as file:
        reader = csv.DictReader(file, lineterminator='\n', delimiter=',')
        for row in reader:
            if not duplicate_key(row['Loc_key'], key_set=KEY_SET)\
                    and not type_error(row) \
                    and unique(row, point_data['coordinates'])\
                    and in_range(row):
                point_data['coordinates'].append([
                    float(row['Longitude']),
                    float(row['Latitude'])]
                )
                key_data.append(row['Loc_key'])
            else:
                bad_stream.write(f"{row['Loc_key']},{row['Latitude']},{row['Longitude']}\n")

        # Add meta to conform to Geojson specifications
        point_data['type'] = 'MultiPoint'

    LOGGER.info(f'Transform time is {1e3 * (time() - start):.2f} ms')

    # Collect all area reachable by foot (within 5 & 10 min)
    # for every coordinate
    # TODO: I/O bottle neck try asyncio (FYI: API limit is 300 request/min)
    start = time()
    isochrone_data = defaultdict(list)
    for point in point_data['coordinates']:
        result = request_isochrone(point)
        isochrone_data['features'].append(result['features'][0])  # 5 min
        isochrone_data['features'].append(result['features'][1])  # 10 min

    # Add meta to conform to Geojson specifications
    isochrone_data['type'] = 'FeatureCollection'

    LOGGER.info(f'Walkable area calculated in: {(time() - start):.2f} s')
    LOGGER.info(f'Number of coordinate points: {len(point_data["coordinates"])}')

    upload_to_mapbox(point_data, type_=f'{tile_name}_points')
    upload_to_mapbox(isochrone_data, type_=f'{tile_name}_polygons')

    # Calculate fridge overlap
    point_data['overlap'] = number_of_overlaps(point_data)

    # Add location keys
    point_data['Loc_key'] = key_data

    with open(CLEAN_DATA, 'w') as f:
        f.write(json.dumps(point_data))

    # Release resources
    del point_data, isochrone_data

    # Write bad rows to file
    with open(BAD_DATA, 'w') as err_data:
        err_data.write(bad_stream.getvalue())
        bad_stream.close()

    LOGGER.info('Done.')


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
