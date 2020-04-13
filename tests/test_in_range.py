"""
Test for function "in_range" found in ./src/main.py
"""

from src.main import in_range


def test_not_in_range():
    coordinates = {'Loc_key': '136', 'Latitude': '9999999', 'Longitude': '9999999'}
    inside_range = in_range(coordinates)
    assert not inside_range


def test_in_range():
    coordinates = {'Loc_key': '34', 'Latitude': '41.8869909', 'Longitude': '-87.6770135'}
    inside_range = in_range(coordinates)
    assert inside_range
