import re

import pytest
from beanie import PydanticObjectId

from nqlstore.mongo import Document, Indexed
from tests.utils import insert_test_data, load_fixture

_LIBRARY_DATA = load_fixture("libraries.json")


class Library(Document):
    address: str
    name: str

    class Settings:
        name = "libraries"


class Book(Document):
    title: Indexed(str)
    library_id: PydanticObjectId

    class Settings:
        name = "books"


@pytest.mark.asyncio
async def test_find(mongo_store):
    """Update should update the items that match the filter"""
    inserted_libs, inserted_books = await insert_test_data(
        mongo_store, library_model=Library, book_model=Book
    )

    got = await mongo_store.find(Library, Library.id != None, skip=1)
    expected = [v for idx, v in enumerate(inserted_libs) if idx >= 1]
    assert got == expected


@pytest.mark.asyncio
async def test_create(mongo_store):
    """Create should add many items to the mongo database"""
    await mongo_store.register([Library, Book])
    got = await mongo_store.insert(Library, _LIBRARY_DATA)
    got = [v.dict(exclude={"id"}) for v in got]
    assert got == _LIBRARY_DATA


@pytest.mark.asyncio
async def test_update(mongo_store):
    """Update should update the items that match the filter"""
    inserted_libs, _ = await insert_test_data(
        mongo_store, library_model=Library, book_model=Book
    )
    updates = {"$set": {"address": "some new address"}}
    filters = {"name": re.compile(r"^b", re.I)}
    startswith_b = lambda v: v.name.lower().startswith("b")
    expected_data_in_db = [
        (record.model_copy(update=updates["$set"]) if startswith_b(record) else record)
        for record in inserted_libs
    ]

    # in immediate response
    got = await mongo_store.update(Library, filters, updates=updates)
    expected = list(filter(startswith_b, expected_data_in_db))
    assert got == expected

    # all library data in database
    got = await mongo_store.find(Library, {})
    assert got == expected_data_in_db


@pytest.mark.asyncio
async def test_delete(mongo_store):
    """Delete should remove the items that match the filter"""
    inserted_libs, _ = await insert_test_data(
        mongo_store, library_model=Library, book_model=Book
    )

    # in immediate response
    got = await mongo_store.delete(Library, {"name": re.compile(r"^b", re.I)})
    expected = [v for v in inserted_libs if v.name.lower().startswith("b")]
    assert got == expected

    # all data in database
    got = await mongo_store.find(Library, {})
    expected = [v for v in inserted_libs if not v.name.lower().startswith("b")]
    assert got == expected
