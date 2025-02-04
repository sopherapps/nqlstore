import pytest

from tests.conftest import RedisBook, RedisLibrary
from tests.utils import insert_test_data, load_fixture

_LIBRARY_DATA = load_fixture("libraries.json")
_TEST_ADDRESS = "Hoima, Uganda"


@pytest.mark.asyncio
async def test_find_native(redis_store, inserted_redis_libs):
    """Find should return the items that match the native filter"""
    got = await redis_store.find(
        RedisLibrary,
        (RedisLibrary.address == _TEST_ADDRESS) | (RedisLibrary.name.startswith("ba")),
        skip=1,
    )
    expected = [
        v
        for v in _sort(inserted_redis_libs)
        if v.address == _TEST_ADDRESS or v.name.lower().startswith("ba")
    ][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_find_mongo_style(redis_store, inserted_redis_libs):
    """Find should return the items that match the mongodb-like filter"""
    got = await redis_store.find(
        RedisLibrary,
        query={"$or": [{"address": {"$eq": _TEST_ADDRESS}}, {"name": {"$eq": "Bar"}}]},
        skip=1,
    )
    expected = [
        v
        for v in _sort(inserted_redis_libs)
        if v.address == _TEST_ADDRESS or v.name == "Bar"
    ][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_find_hybrid(redis_store, inserted_redis_libs):
    """Find should return the items that match the mongodb-like filter AND the native filter"""
    got = await redis_store.find(
        RedisLibrary,
        (RedisLibrary.name.startswith("bu")),
        query={"address": {"$eq": _TEST_ADDRESS}},
        skip=1,
    )
    expected = [
        v
        for v in _sort(inserted_redis_libs)
        if v.address == _TEST_ADDRESS and v.name.lower().startswith("bu")
    ][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_create(redis_store):
    """Create should add many items to the sql database"""
    await redis_store.register([RedisLibrary, RedisBook])
    got = await redis_store.insert(RedisLibrary, _LIBRARY_DATA)
    got = [v.dict(exclude={"pk"}) for v in got]
    assert got == _LIBRARY_DATA


@pytest.mark.asyncio
async def test_update_native(redis_store, inserted_redis_libs):
    """Update should update the items that match the native filter"""
    updates = {"address": "some new address"}
    matches_query = lambda v: v.name.startswith("Bu") and v.address == _TEST_ADDRESS

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await redis_store.update(
        RedisLibrary,
        (RedisLibrary.name.startswith("Bu") & (RedisLibrary.address == _TEST_ADDRESS)),
        updates=updates,
    )
    expected = [
        record.model_copy(update=updates)
        for record in inserted_redis_libs
        if matches_query(record)
    ]
    assert got == expected

    # all library data in database
    got = await redis_store.find(RedisLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_redis_libs
    ]
    assert _sort(got) == _sort(expected)


@pytest.mark.asyncio
async def test_update_mongo_style(redis_store, inserted_redis_libs):
    """Update should update the items that match the mongodb-like filter"""
    updates = {"address": "some new address"}
    matches_query = lambda v: v.name != "Kisaasi" and v.address == _TEST_ADDRESS

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await redis_store.update(
        RedisLibrary,
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
        for record in inserted_redis_libs
        if matches_query(record)
    ]
    assert got == expected

    # all library data in database
    got = await redis_store.find(RedisLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_redis_libs
    ]
    assert _sort(got) == _sort(expected)


@pytest.mark.asyncio
async def test_update_hybrid(redis_store, inserted_redis_libs):
    """Update should update the items that match the mongodb-like filter AND the native filter"""
    updates = {"address": "some new address"}
    matches_query = lambda v: v.name.startswith("Bu") and v.address == _TEST_ADDRESS

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await redis_store.update(
        RedisLibrary,
        (RedisLibrary.name.startswith("Bu")),
        query={"address": {"$eq": _TEST_ADDRESS}},
        updates=updates,
    )
    expected = [
        record.model_copy(update=updates)
        for record in inserted_redis_libs
        if matches_query(record)
    ]
    assert got == expected

    # all library data in database
    got = await redis_store.find(RedisLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_redis_libs
    ]
    assert _sort(got) == _sort(expected)


@pytest.mark.asyncio
async def test_delete_native(redis_store, inserted_redis_libs):
    """Delete should delete the items that match the native filter"""
    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await redis_store.delete(RedisLibrary, RedisLibrary.name.startswith("bu"))
    expected = [v for v in inserted_redis_libs if v.name.lower().startswith("bu")]
    assert got == expected

    # all data in database
    got = await redis_store.find(RedisLibrary)
    expected = [v for v in inserted_redis_libs if not v.name.lower().startswith("bu")]
    assert _sort(got) == _sort(expected)


@pytest.mark.asyncio
async def test_delete_mongo_style(redis_store, inserted_redis_libs):
    """Delete should delete the items that match the mongodb-like filter"""
    addresses = ["Bujumbura, Burundi", "Non existent"]
    unwanted_names = ["Bar", "Kisaasi"]

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await redis_store.delete(
        RedisLibrary,
        query={
            "$or": [
                {"$nor": [{"name": {"$eq": name}} for name in unwanted_names]},
                {"address": {"$in": addresses}},
            ]
        },
    )
    expected = [
        v
        for v in inserted_redis_libs
        if v.address in addresses or v.name not in unwanted_names
    ]
    assert _sort(got) == _sort(expected)

    # all data in database
    got = await redis_store.find(RedisLibrary)
    expected = [
        v
        for v in inserted_redis_libs
        if v.address not in addresses and v.name in unwanted_names
    ]
    assert _sort(got) == _sort(expected)


@pytest.mark.asyncio
async def test_delete_hybrid(redis_store, inserted_redis_libs):
    """Delete should delete the items that match the mongodb-like filter AND the native filter"""
    unwanted_addresses = ["Stockholm, Sweden"]

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await redis_store.delete(
        RedisLibrary,
        (RedisLibrary.name.startswith("bu")),
        query={"address": {"$nin": unwanted_addresses}},
    )
    expected = [
        v
        for v in inserted_redis_libs
        if v.address not in unwanted_addresses and v.name.lower().startswith("bu")
    ]
    assert _sort(got) == _sort(expected)

    # all data in database
    got = await redis_store.find(RedisLibrary)
    expected = [
        v
        for v in inserted_redis_libs
        if v.address in unwanted_addresses or not v.name.lower().startswith("bu")
    ]
    assert _sort(got) == _sort(expected)


def _sort(libraries: list[RedisLibrary]) -> list[RedisLibrary]:
    """Sorts the given libraries by address

    Args:
        libraries: the libraries to sort

    Returns:
        the sorted libraries
    """
    return sorted(libraries, key=lambda v: v.address)
