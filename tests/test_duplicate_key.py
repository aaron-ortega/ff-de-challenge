"""
Test for function "duplicate_key" found in ./src/main.py
"""

from src.main import duplicate_key
import pytest

KEY_SET = set()


@pytest.fixture(scope='session')
def get_keys():
    test_data = [('20', '41.6930851', '-87.8375'),
                 ('20', '41.6930851', '-87.8375')]
    key1 = test_data[0][0]
    key2 = test_data[1][0]
    duplicate = duplicate_key(key1, key_set=KEY_SET)

    yield duplicate, key2


def test_no_duplicate_key(get_keys):
    """Assert that the first key value is not already present"""
    duplicate, _ = get_keys
    assert not duplicate


def test_duplicate_key(get_keys):
    """
    Assert that the second key value (which has the same value as the first key)
    is a duplicate key.
    """
    duplicate, key2 = get_keys
    check_key2 = duplicate_key(key2, key_set=KEY_SET)
    assert check_key2
