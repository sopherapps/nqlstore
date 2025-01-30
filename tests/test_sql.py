import pytest

from nqlstore.sql import Field, SQLModel
from tests.utils import insert_test_data, load_fixture

_LIBRARY_DATA = load_fixture("libraries.json")


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
    inserted_libs, inserted_books = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )

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
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )

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
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )

    # in immediate response
    got = await sql_store.delete(Library, Library.id > 2)
    expected = [v for v in inserted_libs if v.id > 2]
    assert got == expected

    # in database
    got = await sql_store.find(Library, Library.id > -1)
    expected = [v for v in inserted_libs if v.id <= 2]
    assert got == expected
