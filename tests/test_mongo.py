import re

import pytest

from nqlstore._compat import Document
from tests.conftest import MongoBook, MongoLibrary
from tests.utils import is_lib_installed, load_fixture

_LIBRARY_DATA = load_fixture("libraries.json")
_BOOK_DATA = load_fixture("books.json")


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
async def test_register(mongo_store, inserted_mongo_libs):
    """Register ensures that the MongoLibrary is properly initialized"""
    assert (
        MongoLibrary._document_settings.motor_collection.full_name
        == "testing.libraries"
    )
    assert not issubclass(MongoBook, Document)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
async def test_find(mongo_store, inserted_mongo_libs):
    """Find should find the items that match the filter"""
    got = await mongo_store.find(MongoLibrary, {}, skip=1)
    expected = [v for idx, v in enumerate(inserted_mongo_libs) if idx >= 1]
    assert got == expected


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
async def test_find_dot_notation(mongo_store, inserted_mongo_libs):
    """Find should find the items that match the filter with embedded objects"""
    got = await mongo_store.find(
        MongoLibrary, {"books.title": {"$regex": "^be.*", "$options": "i"}}
    )
    expected = [
        v
        for v in inserted_mongo_libs
        if any([bk.title.lower().startswith("be") for bk in v.books])
    ]
    assert got == expected


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
@pytest.mark.parametrize("index", range(4))
async def test_regex_find(mongo_store, regex_params_mongo, index):
    """Find with regex should find the items that match the regex"""
    filters, expected = regex_params_mongo[index]
    got = await mongo_store.find(MongoLibrary, filters)
    assert got == expected


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
async def test_create(mongo_store):
    """Create should add many items to the mongo database"""
    await mongo_store.register([MongoLibrary])
    books = [MongoBook(**v) for v in _BOOK_DATA]
    lib_data = [{**v, "books": [*books]} for v in _LIBRARY_DATA]
    got = await mongo_store.insert(MongoLibrary, lib_data)
    got = [v.model_dump(exclude={"id"}) for v in got]
    expected = [{**v, "books": [*_BOOK_DATA]} for v in _LIBRARY_DATA]
    assert got == expected


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
async def test_update(mongo_store, inserted_mongo_libs):
    """Update should update the items that match the filter"""
    updates = {"address": "some new address"}
    filters = {"name": re.compile(r"^b", re.I)}
    startswith_b = lambda v: v.name.lower().startswith("b")
    expected_data_in_db = [
        (record.model_copy(update=updates) if startswith_b(record) else record)
        for record in inserted_mongo_libs
    ]

    # in immediate response
    got = await mongo_store.update(MongoLibrary, filters, updates=updates)
    expected = list(filter(startswith_b, expected_data_in_db))
    assert got == expected

    # all library data in database
    got = await mongo_store.find(MongoLibrary, {})
    assert got == expected_data_in_db


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
async def test_update_native(mongo_store, inserted_mongo_libs):
    """Update should update the items that match the filter with mongo-style update operators"""
    updates = {"$set": {"address": "some new address"}}
    filters = {"name": re.compile(r"^b", re.I)}
    startswith_b = lambda v: v.name.lower().startswith("b")
    expected_data_in_db = [
        (record.model_copy(update=updates["$set"]) if startswith_b(record) else record)
        for record in inserted_mongo_libs
    ]

    # in immediate response
    got = await mongo_store.update(MongoLibrary, filters, updates=updates)
    expected = list(filter(startswith_b, expected_data_in_db))
    assert got == expected

    # all library data in database
    got = await mongo_store.find(MongoLibrary, {})
    assert got == expected_data_in_db


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
async def test_delete(mongo_store, inserted_mongo_libs):
    """Delete should remove the items that match the filter"""
    # in immediate response
    got = await mongo_store.delete(MongoLibrary, {"name": re.compile(r"^b", re.I)})
    expected = [v for v in inserted_mongo_libs if v.name.lower().startswith("b")]
    assert got == expected

    # all data in database
    got = await mongo_store.find(MongoLibrary, {})
    expected = [v for v in inserted_mongo_libs if not v.name.lower().startswith("b")]
    assert got == expected
