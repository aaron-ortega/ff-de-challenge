"""
Test for function "type_error" found in ./src/main.py
"""

from src.main import type_error


def test_no_type_error():
    coordinates = {'Loc_key': '34', 'Latitude': '41.8869909', 'Longitude': '-87.6770135'}
    error = type_error(coordinates)
    assert not error


def test_type_error():
    coordinates = {'Loc_key': '44', 'Latitude': 'N/A', 'Longitude': 'N/A'}
    error = type_error(coordinates)
    assert error
