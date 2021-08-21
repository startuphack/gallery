import pathlib

import pytest

from utils import pickle_load


@pytest.fixture()
def resources():
    return pathlib.Path(__file__).parent / 'resources'


@pytest.fixture()
def schedule_plan_data(resources):
    return pickle_load(str(resources / 'test_pairs.pkl'))
