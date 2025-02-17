import pytest

from tests.conftest import SqlBook, SqlLibrary
from tests.utils import is_lib_installed, load_fixture

_LIBRARY_DATA = load_fixture("libraries.json")
_TEST_ADDRESS = "Hoima, Uganda"


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def test_model_dump(sql_store, inserted_sql_libs):
    """model_dump should recursively dump any 'embedded' models"""
    got = [v.model_dump() for v in inserted_sql_libs]
    expected = [
        {
            **v.model_dump(exclude={"books"}),
            "books": [bk.model_dump() for bk in v.books],
        }
        for v in inserted_sql_libs
    ]
    assert got == expected


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
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
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
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
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def test_find_dot_notation(sql_store, inserted_sql_libs):
    """Find should find the items that match the filter with embedded objects"""
    wanted_titles = ["Belljar", "Benediction man"]
    matches_query = lambda v: any(bk.title in wanted_titles for bk in v.books)

    got = await sql_store.find(
        SqlLibrary, query={"books.title": {"$in": wanted_titles}}
    )

    expected = [v for v in inserted_sql_libs if matches_query(v)]
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
@pytest.mark.parametrize("index", range(4))
async def test_regex_find_mongo_style(sql_store, regex_params_sql, index):
    """Find with regex should find the items that match the regex"""
    filters, expected = regex_params_sql[index]
    got = await sql_store.find(SqlLibrary, query=filters)
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
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
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def test_create(sql_store):
    """Create should add many items to the sql database"""
    await sql_store.register([SqlLibrary, SqlBook])
    got = await sql_store.insert(SqlLibrary, _LIBRARY_DATA)
    expected = [
        SqlLibrary(id=idx + 1, **item) for idx, item in enumerate(_LIBRARY_DATA)
    ]
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def test_update_native(sql_store, inserted_sql_libs):
    """Update should update the items that match the native filter"""
    updates = {
        "address": "some new address",
        "books": [
            {"title": "Upon this mountain"},
            {"title": "No longer at ease"},
        ],
    }
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
    assert _ordered(got) == _ordered(expected)

    # all library data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_sql_libs
    ]
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def test_update_mongo_style(sql_store, inserted_sql_libs):
    """Update should update the items that match the mongodb-like filter"""
    updates = {
        "address": "some new address",
        "books": [
            {"title": "Upon this mountain"},
            {"title": "No longer at ease"},
        ],
    }
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

    assert _ordered(got) == _ordered(expected)

    # all library data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_sql_libs
    ]
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def test_update_hybrid(sql_store, inserted_sql_libs):
    """Update should update the items that match the mongodb-like filter AND the native filter"""
    updates = {
        "address": "some new address",
        "books": [
            {"title": "Upon this mountain"},
            {"title": "No longer at ease"},
        ],
    }
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
    assert _ordered(got) == _ordered(expected)

    # all library data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_sql_libs
    ]
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def test_update_dot_notation(sql_store, inserted_sql_libs):
    """Update should update the items that match the filter with embedded objects"""
    wanted_titles = ["Belljar", "Benediction man"]
    updates = {
        "address": "some new address",
        "books": [
            {"title": "Upon this mountain"},
            {"title": "No longer at ease"},
        ],
    }
    matches_query = lambda v: any(bk.title in wanted_titles for bk in v.books)

    got = await sql_store.update(
        SqlLibrary,
        query={"books.title": {"$in": wanted_titles}},
        updates=updates,
    )
    expected = [
        record.model_copy(update=updates)
        for record in inserted_sql_libs
        if matches_query(record)
    ]
    assert _ordered(got) == _ordered(expected)

    # all library data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        (record.model_copy(update=updates) if matches_query(record) else record)
        for record in inserted_sql_libs
    ]
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def test_delete_native(sql_store, inserted_sql_libs):
    """Delete should delete the items that match the native filter"""
    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await sql_store.delete(SqlLibrary, SqlLibrary.address.startswith("Ho"))
    expected = [v for v in inserted_sql_libs if v.address.startswith("Ho")]
    assert _ordered(got) == _ordered(expected)

    # all data in database
    got = await sql_store.find(SqlLibrary)
    expected = [v for v in inserted_sql_libs if not v.address.startswith("Ho")]
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
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
    assert _ordered(got) == _ordered(expected)

    # all data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        v
        for v in inserted_sql_libs
        if v.address not in addresses and v.name in unwanted_names
    ]
    assert _ordered(got) == _ordered(expected)


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
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
    assert _ordered(got) == _ordered(expected)

    # all data in database
    got = await sql_store.find(SqlLibrary)
    expected = [
        v
        for v in inserted_sql_libs
        if v.address in unwanted_addresses or not v.name.lower().startswith("bu")
    ]
    assert got == expected


@pytest.mark.asyncio
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def test_delete_dot_notation(sql_store, inserted_sql_libs):
    """Delete should delete the items that match the filter with embedded objects"""
    wanted_titles = ["Belljar", "Benediction man"]
    matches_query = lambda v: any(bk.title in wanted_titles for bk in v.books)

    got = await sql_store.delete(
        SqlLibrary,
        query={"books.title": {"$in": wanted_titles}},
    )
    expected = [record for record in inserted_sql_libs if matches_query(record)]
    assert _ordered(got) == _ordered(expected)

    # all library data in database
    got = await sql_store.find(SqlLibrary)
    expected = [record for record in inserted_sql_libs if not matches_query(record)]
    assert _ordered(got) == _ordered(expected)


def _ordered(libs: list[SqlLibrary]) -> list[SqlLibrary]:
    """Sorts the libraries by id and returns them

    Args:
        libs: the library instances to sort

    Returns:
        the ordered libraries
    """
    return sorted(libs, key=lambda v: v.id)
