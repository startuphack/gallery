import json
import pathlib

import pytest


@pytest.fixture()
def resources():
    return pathlib.Path(__file__).parent / 'resources'


def replace_keys_to_int(dct):
    return {int(k) if k.isdigit() else k: replace_keys_to_int(v) if isinstance(v, dict) else v for k, v in dct.items()}


@pytest.fixture()
def schedule_plan_data(resources):
    with open(str(resources / 'test_pairs.json'), encoding='utf8') as f:
        result = json.load(f)

        return replace_keys_to_int(result)
