import pytest

from tests.conftest import RedisBook, RedisLibrary
from tests.utils import is_lib_installed, load_fixture

_LIBRARY_DATA = load_fixture("libraries.json")
_BOOK_DATA = load_fixture("books.json")
_TEST_ADDRESS = "Hoima, Uganda"


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
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
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
async def test_find_dot_notation(redis_store, inserted_redis_libs):
    """Find should find the items that match the filter with embedded objects"""
    wanted_titles = ["Belljar", "Benediction man"]
    matches_query = lambda v: any(bk.title in wanted_titles for bk in v.books)

    got = await redis_store.find(
        RedisLibrary, query={"books.title": {"$in": wanted_titles}}
    )

    expected = [v for v in inserted_redis_libs if matches_query(v)]
    assert got == expected


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
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
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
@pytest.mark.parametrize("index", range(4))
async def test_regex_find_mongo_style(redis_store, regex_params_redis, index):
    """Find with regex should find the items that match the regex"""
    filters, expected = regex_params_redis[index]
    with pytest.raises(
        NotImplementedError, match=r"redis text search is too inexpressive for regex."
    ):
        await redis_store.find(RedisLibrary, query=filters)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
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
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
async def test_create(redis_store):
    """Create should add many items to the sql database"""
    await redis_store.register([RedisLibrary, RedisBook])
    books = [RedisBook(**v) for v in _BOOK_DATA]
    lib_data = [{**v, "books": [*books]} for v in _LIBRARY_DATA]
    got = await redis_store.insert(RedisLibrary, lib_data)
    got = [v.model_dump(exclude={"pk", "id"}) for v in got]
    expected = [
        {**v, "books": [bk.model_dump() for bk in books]} for v in _LIBRARY_DATA
    ]
    assert got == expected


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
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
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
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
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
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
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
async def test_update_dot_notation(redis_store, inserted_redis_libs):
    """Update should update the items that match the filter with embedded objects"""
    wanted_titles = ["Belljar", "Benediction man"]
    updates = {"address": "some new address"}
    matches_query = lambda v: any(bk.title in wanted_titles for bk in v.books)

    got = await redis_store.update(
        RedisLibrary,
        query={"books.title": {"$in": wanted_titles}},
        updates=updates,
    )
    expected = [
        record.model_copy(update=updates)
        for record in inserted_redis_libs
        if matches_query(record)
    ]
    assert _sort(got) == _sort(expected)

    # all library data in database
    got = await redis_store.find(RedisLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_redis_libs
    ]
    assert _sort(got) == _sort(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
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
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
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
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
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


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
async def test_delete_dot_notation(redis_store, inserted_redis_libs):
    """Delete should delete the items that match the filter with embedded objects"""
    wanted_titles = ["Belljar", "Benediction man"]
    matches_query = lambda v: any(bk.title in wanted_titles for bk in v.books)

    got = await redis_store.delete(
        RedisLibrary,
        query={"books.title": {"$in": wanted_titles}},
    )
    expected = [record for record in inserted_redis_libs if matches_query(record)]
    assert _sort(got) == _sort(expected)

    # all library data in database
    got = await redis_store.find(RedisLibrary)
    expected = [record for record in inserted_redis_libs if not matches_query(record)]
    assert _sort(got) == _sort(expected)


def _sort(libraries: list[RedisLibrary]) -> list[RedisLibrary]:
    """Sorts the given libraries by address

    Args:
        libraries: the libraries to sort

    Returns:
        the sorted libraries
    """
    return sorted(libraries, key=lambda v: v.address)
