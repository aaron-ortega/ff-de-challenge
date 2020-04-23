"""
Functions used in ./src/main.py to validate data
"""


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
