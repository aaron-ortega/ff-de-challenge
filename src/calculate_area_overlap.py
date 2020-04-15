"""
This script does the following:
    1. Queries a tileset
    2.
"""

from mapbox import Tilequery
from src.utils import get_credentials

CONFIG = get_credentials()
TOKEN = CONFIG.get('mapbox-api', 'token')
LAT = 41.7889297
LONG = -87.595527
LAYER = 'chicago_milwaukee_data_polygons'
TILESET_ID = f'aortega04.{LAYER}'

radius = 1.4 * 60 * 10  # Avg walk speed 1.4 (m/sec) * 60 (sec/min) * 10 (min) = 840 m (distance traveled in 10 min)


tile_query = Tilequery(access_token=TOKEN)
response = tile_query.tilequery(TILESET_ID, lon=LONG, lat=LAT, radius=radius)

if response.status_code == 200:
    print(response.json())
