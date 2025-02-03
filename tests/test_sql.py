import pytest

from nqlstore.sql import Field, SQLModel
from tests.utils import insert_test_data, load_fixture

_LIBRARY_DATA = load_fixture("libraries.json")
_TEST_ADDRESS = "Hoima, Uganda"


class Library(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    address: str
    name: str


class Book(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    title: str
    library_id: int = Field(default=None, foreign_key="library.id")


@pytest.mark.asyncio
async def test_find_native(sql_store):
    """Find should return the items that match the native filter"""
    inserted_libs, inserted_books = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )

    got = await sql_store.find(
        Library,
        (Library.address == _TEST_ADDRESS) | (Library.name.startswith("Ba")),
        skip=1,
    )
    expected = [
        v
        for v in inserted_libs
        if v.address == _TEST_ADDRESS or v.name.startswith("Ba")
    ][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_find_mongo_style(sql_store):
    """Find should return the items that match the mongodb-like filter"""
    inserted_libs, inserted_books = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )

    got = await sql_store.find(
        Library,
        query={"$or": [{"address": {"$eq": _TEST_ADDRESS}}, {"name": {"$eq": "Bar"}}]},
        skip=1,
    )
    expected = [
        v for v in inserted_libs if v.address == _TEST_ADDRESS or v.name == "Bar"
    ][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_find_hybrid(sql_store):
    """Find should return the items that match the mongodb-like filter AND the native filter"""
    inserted_libs, inserted_books = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )

    got = await sql_store.find(
        Library,
        (Library.name.startswith("Ba")),
        query={"address": {"$eq": _TEST_ADDRESS}},
        skip=1,
    )
    expected = [
        v
        for v in inserted_libs
        if v.address == _TEST_ADDRESS and v.name.startswith("Ba")
    ][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_create(sql_store):
    """Create should add many items to the sql database"""
    await sql_store.register([Library, Book])
    got = await sql_store.insert(Library, _LIBRARY_DATA)
    expected = [Library(id=idx + 1, **item) for idx, item in enumerate(_LIBRARY_DATA)]
    assert got == expected


@pytest.mark.asyncio
async def test_update_native(sql_store):
    """Update should update the items that match the native filter"""
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )
    updates = {"address": "some new address"}
    matches_query = lambda v: v.name.startswith("Bu") and v.address == _TEST_ADDRESS

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.update(
        Library,
        (Library.name.startswith("Bu") & (Library.address == _TEST_ADDRESS)),
        updates=updates,
    )
    expected = [
        record.model_copy(update=updates)
        for record in inserted_libs
        if matches_query(record)
    ]
    assert got == expected

    # all library data in database
    got = await sql_store.find(Library)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_libs
    ]
    assert got == expected


@pytest.mark.asyncio
async def test_update_mongo_style(sql_store):
    """Update should update the items that match the mongodb-like filter"""
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )
    updates = {"address": "some new address"}
    matches_query = lambda v: v.name != "Kisaasi" and v.address == _TEST_ADDRESS

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.update(
        Library,
        query={
            "$and": [
                {"name": {"$not": {"$eq": "Kisaasi"}}},
                {"address": {"$eq": _TEST_ADDRESS}},
            ]
        },
        updates=updates,
    )
    expected = [
        record.model_copy(update=updates)
        for record in inserted_libs
        if matches_query(record)
    ]

    assert got == expected

    # all library data in database
    got = await sql_store.find(Library)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_libs
    ]
    assert got == expected


@pytest.mark.asyncio
async def test_update_hybrid(sql_store):
    """Update should update the items that match the mongodb-like filter AND the native filter"""
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )
    updates = {"address": "some new address"}
    matches_query = lambda v: v.name.startswith("Bu") and v.address == _TEST_ADDRESS

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.update(
        Library,
        (Library.name.startswith("Bu")),
        query={"address": {"$eq": _TEST_ADDRESS}},
        updates=updates,
    )
    expected = [
        record.model_copy(update=updates)
        for record in inserted_libs
        if matches_query(record)
    ]
    assert got == expected

    # all library data in database
    got = await sql_store.find(Library)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_libs
    ]
    assert got == expected


@pytest.mark.asyncio
async def test_delete_native(sql_store):
    """Delete should delete the items that match the native filter"""
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.delete(Library, Library.name.startswith("bu"))
    expected = [v for v in inserted_libs if v.name.lower().startswith("bu")]
    assert got == expected

    # all data in database
    got = await sql_store.find(Library)
    expected = [v for v in inserted_libs if not v.name.lower().startswith("bu")]
    assert got == expected


@pytest.mark.asyncio
async def test_delete_mongo_style(sql_store):
    """Delete should delete the items that match the mongodb-like filter"""
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )
    addresses = ["Bujumbura, Burundi", "Non existent"]
    unwanted_names = ["Bar", "Kisaasi"]

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.delete(
        Library,
        query={
            "$or": [
                {"$nor": [{"name": {"$eq": name}} for name in unwanted_names]},
                {"address": {"$in": addresses}},
            ]
        },
    )
    expected = [
        v
        for v in inserted_libs
        if v.address in addresses or v.name not in unwanted_names
    ]
    assert got == expected

    # all data in database
    got = await sql_store.find(Library)
    expected = [
        v
        for v in inserted_libs
        if v.address not in addresses and v.name in unwanted_names
    ]
    assert got == expected


@pytest.mark.asyncio
async def test_delete_hybrid(sql_store):
    """Delete should delete the items that match the mongodb-like filter AND the native filter"""
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=Library, book_model=Book
    )
    unwanted_addresses = ["Stockholm, Sweden"]

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.delete(
        Library,
        (Library.name.startswith("bu")),
        query={"address": {"$nin": unwanted_addresses}},
    )
    expected = [
        v
        for v in inserted_libs
        if v.address not in unwanted_addresses and v.name.lower().startswith("bu")
    ]
    assert got == expected

    # all data in database
    got = await sql_store.find(Library)
    expected = [
        v
        for v in inserted_libs
        if v.address in unwanted_addresses or not v.name.lower().startswith("bu")
    ]
    assert got == expected
