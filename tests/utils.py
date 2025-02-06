import json
import re
from os import path
from typing import Any, TypeVar

from beanie import PydanticObjectId
from pydantic import BaseModel
from sqlalchemy.sql._typing import _ColumnExpressionArgument

from nqlstore._base import BaseStore
from nqlstore._sql import SQLModel, select

_SQLFilter = _ColumnExpressionArgument[bool] | bool
_TESTS_FOLDER = path.dirname(path.abspath(__file__))
_FIXTURES_PATH = path.join(_TESTS_FOLDER, "fixtures")
_LibType = TypeVar("_LibType", bound=BaseModel)
_BookType = TypeVar("_BookType", bound=BaseModel)


def load_fixture(fixture_name: str) -> list[dict[str, Any]] | dict[str, Any]:
    """Load fixture and return it as python objects

    Args:
        fixture_name: the name of the fixture file name

    Returns:
        the fixture as python objects
    """
    file_path = path.join(_FIXTURES_PATH, fixture_name)
    with open(file_path, "rb") as file:
        return json.load(file)


async def insert_test_data(
    store: BaseStore, library_model: type[_LibType], book_model: type[_BookType]
) -> tuple[list[_LibType], list[_BookType]]:
    """Insert data in the database before tests

    Args:
        store: the store to insert test data in
        library_model: the model class for the RedisLibrary
        book_model: the model class for the RedisBook

    Returns:
        the inserted data as a tuple of libraries, books
    """
    library_data = load_fixture("libraries.json")
    book_data = load_fixture("books.json")

    await store.register([library_model, book_model])
    libraries = await store.insert(library_model, library_data)

    book_data = [
        book_model(library_id=_get_id(libraries[idx % 2]), **data)
        for idx, data in enumerate(book_data)
    ]
    books = await store.insert(book_model, book_data)

    return libraries, books


def _get_id(lib: _LibType) -> PydanticObjectId | int | str:
    """Gets the id of the lib

    Args:
        lib: the library to extract the id from

    Returns:
        the id or pk
    """
    try:
        return lib.id
    except AttributeError:
        return lib.pk


def to_sql_text(model: type[SQLModel], queries: tuple[_SQLFilter, ...]) -> str:
    """Converts a tuple of sql filters into their sql text

    It assumes the queries are to be used in a find operation

    Args:
        model: the model to operate on
        queries: the filters to use on the data

    Returns:
        the sql as text if this were for a find operation
    """
    sql = select(model).where(*queries)
    return str(sql.compile())


def get_regex_test_params(libs: list[_LibType]) -> list[tuple[dict, list[_LibType]]]:
    """Generates the test params for the REGEX test for the given libs

    Args:
        libs: the Library records

    Returns:
        pairs of regex filter and expected output after querying
    """
    return [
        (
            {"name": {"$regex": "^bu.*", "$options": "i"}},
            [v for v in libs if re.match(r"^bu.*", v.name, re.I)],
        ),
        (
            {"name": {"$regex": "^bu.*"}},
            [v for v in libs if re.match(r"^bu.*", v.name)],
        ),
        (
            {"name": {"$regex": "am.*"}},
            [v for v in libs if re.match(r".*am.*", v.name)],
        ),
        (
            {"name": {"$regex": ".*i$"}},
            [v for v in libs if re.match(r".*i$", v.name)],
        ),
    ]
