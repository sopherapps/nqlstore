from collections import namedtuple

import pytest

from nqlstore.sql import Field, SQLModel, SQLStore
from tests.utils import load_fixture

_TestData = namedtuple("_TestData", ["libraries", "books"])

_LIBRARY_DATA = load_fixture("libraries.json")
_BOOK_DATA = load_fixture("books.json")


class Library(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    address: str
    name: str


class Book(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    title: str
    library_id: int = Field(default=None, foreign_key="library.id")


@pytest.mark.asyncio
async def test_find(sql_store):
    """Update should update the items that match the filter"""
    inserted_libs, inserted_books = await _insert_test_data(sql_store)

    # in immediate response
    got = await sql_store.find(Library, Library.id > 1, skip=1)
    expected = [v for v in inserted_libs if v.id > 2]
    assert got == expected


@pytest.mark.asyncio
async def test_create(sql_store):
    """Create should add many items to the sql database"""
    await sql_store.register([Library, Book])
    got = await sql_store.insert(Library, _LIBRARY_DATA)
    expected = [Library(id=idx + 1, **item) for idx, item in enumerate(_LIBRARY_DATA)]
    assert got == expected


@pytest.mark.asyncio
async def test_update(sql_store):
    """Update should update the items that match the filter"""
    inserted_libs, _ = await _insert_test_data(sql_store)

    updates = {"address": "some new address"}

    # in immediate response
    got = await sql_store.update(Library, Library.id > 2, updates=updates)
    expected = [
        Library(**{**v.model_dump(), **updates}) for v in inserted_libs if v.id > 2
    ]
    assert got == expected

    # in database
    got = await sql_store.find(Library, Library.id > -1)
    expected = [
        v if v.id <= 2 else Library(**{**v.model_dump(), **updates})
        for v in inserted_libs
    ]
    assert got == expected


@pytest.mark.asyncio
async def test_delete(sql_store):
    """Delete should remove the items that match the filter"""
    inserted_libs, _ = await _insert_test_data(sql_store)

    # in immediate response
    got = await sql_store.delete(Library, Library.id > 2)
    expected = [v for v in inserted_libs if v.id > 2]
    assert got == expected

    # in database
    got = await sql_store.find(Library, Library.id > -1)
    expected = [v for v in inserted_libs if v.id <= 2]
    assert got == expected


async def _insert_test_data(store: SQLStore):
    """Insert data in the database before tests"""
    await store.register([Library, Book])
    libraries = await store.insert(Library, _LIBRARY_DATA)

    book_data = [
        Book(library_id=libraries[idx % 2].id, **data)
        for idx, data in enumerate(_BOOK_DATA)
    ]
    books = await store.insert(Book, book_data)

    return _TestData(libraries, books)
