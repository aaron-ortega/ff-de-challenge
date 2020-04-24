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
from mapbox import Tilequery, Uploader
from prefect import task, Flow
from src.validate_fns import duplicate_key, in_range, type_error, unique
from src.utils import start_logging, get_credentials
from time import time, sleep

LOGGER = start_logging(level='INFO', log_name=__name__)
KEY_SET = set()
CONFIG = get_credentials()
TOKEN = CONFIG.get('mapbox-api', 'token')
USERNAME = CONFIG.get('mapbox-api', 'username')
LAYER = 'chicago_milwaukee_data_polygons'
TILESET_ID = f'{USERNAME}.{LAYER}'
DATA = '../data/FridgeGeo150.csv'
BAD_DATA = '../data/bad_data.csv'
CLEAN_DATA = '../data/cleaned_data.json'

# Distance between point and polygon overlap
RADIUS = 0


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


@task
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


@task
def transform_data(dirty_data):
    # Words
    start = time()
    key_data = []
    point_data = defaultdict(list)
    bad_stream = StringIO()
    bad_stream.write('Loc_key,Latitude,Longitude\n')  # Add header
    with open(dirty_data) as file:
        reader = csv.DictReader(file, lineterminator='\n', delimiter=',')
        for row in reader:
            if not duplicate_key(row['Loc_key'], key_set=KEY_SET) \
                    and not type_error(row) \
                    and unique(row, point_data['coordinates']) \
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

    # Add location keys
    point_data['Loc_key'] = key_data

    LOGGER.info(f'Transform time is {1e3 * (time() - start):.2f} ms')
    LOGGER.info(f'Number of coordinate points: {len(point_data["coordinates"])}')
    return point_data, bad_stream


@task
def extract_isochrone_data(data):
    start = time()
    isochrone_data = defaultdict(list)
    for point in data['coordinates']:
        result = request_isochrone(point)
        isochrone_data['features'].append(result['features'][0])  # 5 min
        isochrone_data['features'].append(result['features'][1])  # 10 min

    # Add meta to conform to Geojson specifications
    isochrone_data['type'] = 'FeatureCollection'
    LOGGER.info(f'Walkable area calculated in: {(time() - start):.2f} s')
    return isochrone_data


@task
def save_bad_data(bad_data):
    # Write bad rows to file
    with open(BAD_DATA, 'w') as err_data:
        err_data.write(bad_data.getvalue())
        bad_data.close()


@task
def number_of_overlaps(data):
    """
    Count the number of polygons (walkable area) that overlaps a point (fridge)
    Args:
        data: dict w/fridge coordinates

    Returns:
        count - list w/number of overlaps for each point
    """
    if isinstance(data, defaultdict) and 'coordinates' in data.keys():
        count = []
        tile_query = Tilequery(access_token=TOKEN)
        for datum in data['coordinates']:
            response = tile_query.tilequery(TILESET_ID,
                                            lon=datum[0], lat=datum[1], radius=RADIUS, geometry='polygon', limit=50)

            if response.status_code == 200:
                overlap_count = len(response.json()['features']) - 2  # see "How to calculate overlap"
                count.append(overlap_count)

        # TODO: add handling of other status codes (400, 500, etc.)
        return count

    else:
        raise Exception('data must be a dict-like obj with a "coordinates" key')


@task
def save_data(overlaps, data):
    data['overlaps'] = overlaps
    with open(CLEAN_DATA, 'w') as f:
        f.write(json.dumps(data))


def main(tile_name):
    """
    Capture valid data, pipe it to a BytesIO stream, and store it to our Mapbox account.
    Capture invalid data, pipe it to a StringIO stream, and store locally for examination.
    """
    with Flow('ff-de-challenge') as flow:
        transformed = transform_data(DATA)
        walkable_areas = extract_isochrone_data(transformed[0])
        save_bad_data(transformed[1])

        upload_to_mapbox(transformed[0], type_=f'{tile_name}_points')
        upload_to_mapbox(walkable_areas, type_=f'{tile_name}_polygons')

        # Calculate fridge overlap
        overlaps = number_of_overlaps(transformed[0])
        save_data(overlaps, transformed[0])
        # flow.visualize()
        flow.run()
    # Collect all area reachable by foot (within 5 & 10 min)
    # for every coordinate
    # TODO: I/O bottle neck try asyncio (FYI: API limit is 300 request/min)

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
        LOGGER.exception('Error')
