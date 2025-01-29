import json
from os import path
from typing import Any, Dict, List

_TESTS_FOLDER = path.dirname(path.abspath(__file__))
_FIXTURES_PATH = path.join(_TESTS_FOLDER, "fixtures")


def load_fixture(fixture_name: str) -> List[Dict[str, Any]] | Dict[str, Any]:
    """Load fixture and return it as python objects

    Args:
        fixture_name: the name of the fixture file name

    Returns:
        the fixture as python objects
    """
    file_path = path.join(_FIXTURES_PATH, fixture_name)
    with open(file_path, "rb") as file:
        return json.load(file)
