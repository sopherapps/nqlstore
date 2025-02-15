import copy
import importlib
import json
import re
from os import path
from typing import Any, TypeVar

from pydantic import BaseModel

from nqlstore._base import BaseStore
from nqlstore._compat import PydanticObjectId, _ColumnExpressionArgument
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
    store: BaseStore,
    library_model: type[_LibType],
    book_model: type[_BookType],
) -> list[_LibType]:
    """Insert data in the database before tests

    Args:
        store: the store to insert test data in
        library_model: the model class for the Library
        book_model: the model class for the Book

    Returns:
        the inserted libraries (with books embedded)
    """
    library_data = load_fixture("libraries.json")
    book_data = load_fixture("books.json")
    await store.register([library_model, book_model])

    library_data = _embed_test_books(book_model, libs=library_data, books=book_data)
    libraries = await store.insert(library_model, library_data)
    return libraries


def _populate_libs_with_books(
    libs: list[_LibType], books: list[_BookType]
) -> list[_LibType]:
    """Gets the list of libraries with their book lists updated to respective books

    Args:
        libs: the list of libraries
        books: the list of books

    Returns:
        list of libraries with their "books" property populated
    """
    books_per_lib = {}
    for bk in books:
        lib_books = books_per_lib.setdefault(bk.library_id, [])
        lib_books.append(bk)

    return [
        lib.model_copy(update={"books": books_per_lib.get(lib.id, [])}) for lib in libs
    ]


def _embed_test_books(
    model: type[_BookType], libs: list[dict], books: list[dict]
) -> list[dict]:
    """Embeds the books into the libraries

    Args:
        model: the model for the books
        libs: the list of library dicts
        books: the list of book dicts

    Returns:
        the list of library dicts with the books embedded
    """
    libs_copy = copy.deepcopy(libs)

    for idx, data in enumerate(books):
        try:
            libs_copy[idx % 2]["books"].append(model(**data))
        except KeyError:
            libs_copy[idx % 2]["books"] = [{**data}]

    return libs_copy


def _attach_test_books(
    model: type[_BookType], books: list[dict], libs: list[_LibType]
) -> list[_BookType]:
    """Attaches test books to libraries in a predetermined format

    Args:
        model: the model for the books
        books: the list of records to insert
        libs: the list of library instances to attach the books to

    Returns:
        the attached books
    """
    return [
        model(library_id=libs[idx % 2].id, **data) for idx, data in enumerate(books)
    ]


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


def is_lib_installed(lib: str) -> bool:
    """Check if a library is installed.

    Args:
        lib: the library to check

    Returns:
        True if library is installed else False
    """
    return importlib.util.find_spec(lib) is not None
