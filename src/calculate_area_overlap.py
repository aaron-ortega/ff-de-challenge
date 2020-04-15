"""
This script calls Mapbox API and queries if a point is touching a geometry.

What we did:
    In main.py we constructed polygons that illustrate the distance one can
    travel after 5 and 10 mins starting from a fridge location. These polygons
    were all saved into a tileset called "chicago_milwaukee_data_polygons".

What we want:
    Now we want to find how many N polygons overlap a single point.
    Note: The true value should be (N-2) because the point already has two
    polygons that it centers (ie the ones built in main.py).

Tools used:
    Mapbox's Tilequery feature which builds a circle of radius R about a
    point and returns a list of geometries that intersect the circle.

How to calculate overlap:
    If a polygon overlaps a point, then the distance from each other
    will be zero. So we use pass R = 0 to find max overlap per point.
"""

from mapbox import Tilequery
from src.utils import get_credentials

CONFIG = get_credentials()
TOKEN = CONFIG.get('mapbox-api', 'token')
LAT = 41.7889297
LONG = -87.595527
LAYER = 'chicago_milwaukee_data_polygons'
TILESET_ID = f'aortega04.{LAYER}'

# Distance between point and polygon overlap
radius = 0

tile_query = Tilequery(access_token=TOKEN)
response = tile_query.tilequery(TILESET_ID, lon=LONG, lat=LAT, radius=radius, geometry='polygon', limit=50)

if response.status_code == 200:
    print(response.json())
