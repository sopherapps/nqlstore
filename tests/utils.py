import json
from os import path
from typing import Any, TypeVar

from pydantic import BaseModel

from nqlstore._base import BaseStore

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
        library_model: the model class for the Library
        book_model: the model class for the Book

    Returns:
        the inserted data as a tuple of libraries, books
    """
    library_data = load_fixture("libraries.json")
    book_data = load_fixture("books.json")

    await store.register([library_model, book_model])
    libraries = await store.insert(library_model, library_data)

    book_data = [
        book_model(library_id=libraries[idx % 2].id, **data)
        for idx, data in enumerate(book_data)
    ]
    books = await store.insert(book_model, book_data)

    return libraries, books
