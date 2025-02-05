import pytest

from tests.conftest import SqlBook, SqlLibrary
from tests.utils import load_fixture

_LIBRARY_DATA = load_fixture("libraries.json")
_TEST_ADDRESS = "Hoima, Uganda"


@pytest.mark.asyncio
async def test_find_native(sql_store, inserted_sql_libs):
    """Find should return the items that match the native filter"""
    got = await sql_store.find(
        SqlLibrary,
        (SqlLibrary.address == _TEST_ADDRESS) | (SqlLibrary.name.startswith("Ba")),
        skip=1,
    )
    expected = [
        v
        for v in inserted_sql_libs
        if v.address == _TEST_ADDRESS or v.name.startswith("Ba")
    ][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_find_mongo_style(sql_store, inserted_sql_libs):
    """Find should return the items that match the mongodb-like filter"""
    got = await sql_store.find(
        SqlLibrary,
        query={"$or": [{"address": {"$eq": _TEST_ADDRESS}}, {"name": {"$eq": "Bar"}}]},
        skip=1,
    )
    expected = [
        v for v in inserted_sql_libs if v.address == _TEST_ADDRESS or v.name == "Bar"
    ][1:]
    assert got == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(4))
async def test_regex_find_mongo_style(sql_store, regex_params_sql, index):
    """Find with regex should find the items that match the regex"""
    filters, expected = regex_params_sql[index]
    got = await sql_store.find(SqlLibrary, query=filters)
    assert got == expected


@pytest.mark.asyncio
async def test_find_hybrid(sql_store, inserted_sql_libs):
    """Find should return the items that match the mongodb-like filter AND the native filter"""
    got = await sql_store.find(
        SqlLibrary,
        (SqlLibrary.name.startswith("Ba")),
        query={"address": {"$eq": _TEST_ADDRESS}},
        skip=1,
    )
    expected = [
        v
        for v in inserted_sql_libs
        if v.address == _TEST_ADDRESS and v.name.startswith("Ba")
    ][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_create(sql_store):
    """Create should add many items to the sql database"""
    await sql_store.register([SqlLibrary, SqlBook])
    got = await sql_store.insert(SqlLibrary, _LIBRARY_DATA)
    expected = [
        SqlLibrary(id=idx + 1, **item) for idx, item in enumerate(_LIBRARY_DATA)
    ]
    assert got == expected


@pytest.mark.asyncio
async def test_update_native(sql_store, inserted_sql_libs):
    """Update should update the items that match the native filter"""
    updates = {"address": "some new address"}
    matches_query = lambda v: v.name.startswith("Bu") and v.address == _TEST_ADDRESS

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.update(
        SqlLibrary,
        (SqlLibrary.name.startswith("Bu") & (SqlLibrary.address == _TEST_ADDRESS)),
        updates=updates,
    )
    expected = [
        record.model_copy(update=updates)
        for record in inserted_sql_libs
        if matches_query(record)
    ]
    assert got == expected

    # all library data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_sql_libs
    ]
    assert got == expected


@pytest.mark.asyncio
async def test_update_mongo_style(sql_store, inserted_sql_libs):
    """Update should update the items that match the mongodb-like filter"""
    updates = {"address": "some new address"}
    matches_query = lambda v: v.name != "Kisaasi" and v.address == _TEST_ADDRESS

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.update(
        SqlLibrary,
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
        for record in inserted_sql_libs
        if matches_query(record)
    ]

    assert got == expected

    # all library data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_sql_libs
    ]
    assert got == expected


@pytest.mark.asyncio
async def test_update_hybrid(sql_store, inserted_sql_libs):
    """Update should update the items that match the mongodb-like filter AND the native filter"""
    updates = {"address": "some new address"}
    matches_query = lambda v: v.name.startswith("Bu") and v.address == _TEST_ADDRESS

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.update(
        SqlLibrary,
        (SqlLibrary.name.startswith("Bu")),
        query={"address": {"$eq": _TEST_ADDRESS}},
        updates=updates,
    )
    expected = [
        record.model_copy(update=updates)
        for record in inserted_sql_libs
        if matches_query(record)
    ]
    assert got == expected

    # all library data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_sql_libs
    ]
    assert got == expected


@pytest.mark.asyncio
async def test_delete_native(sql_store, inserted_sql_libs):
    """Delete should delete the items that match the native filter"""
    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.delete(SqlLibrary, SqlLibrary.name.startswith("bu"))
    expected = [v for v in inserted_sql_libs if v.name.lower().startswith("bu")]
    assert got == expected

    # all data in database
    got = await sql_store.find(SqlLibrary)
    expected = [v for v in inserted_sql_libs if not v.name.lower().startswith("bu")]
    assert got == expected


@pytest.mark.asyncio
async def test_delete_mongo_style(sql_store, inserted_sql_libs):
    """Delete should delete the items that match the mongodb-like filter"""
    addresses = ["Bujumbura, Burundi", "Non existent"]
    unwanted_names = ["Bar", "Kisaasi"]

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.delete(
        SqlLibrary,
        query={
            "$or": [
                {"$nor": [{"name": {"$eq": name}} for name in unwanted_names]},
                {"address": {"$in": addresses}},
            ]
        },
    )
    expected = [
        v
        for v in inserted_sql_libs
        if v.address in addresses or v.name not in unwanted_names
    ]
    assert got == expected

    # all data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        v
        for v in inserted_sql_libs
        if v.address not in addresses and v.name in unwanted_names
    ]
    assert got == expected


@pytest.mark.asyncio
async def test_delete_hybrid(sql_store, inserted_sql_libs):
    """Delete should delete the items that match the mongodb-like filter AND the native filter"""
    unwanted_addresses = ["Stockholm, Sweden"]

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.delete(
        SqlLibrary,
        (SqlLibrary.name.startswith("bu")),
        query={"address": {"$nin": unwanted_addresses}},
    )
    expected = [
        v
        for v in inserted_sql_libs
        if v.address not in unwanted_addresses and v.name.lower().startswith("bu")
    ]
    assert got == expected

    # all data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        v
        for v in inserted_sql_libs
        if v.address in unwanted_addresses or not v.name.lower().startswith("bu")
    ]
    assert got == expected
