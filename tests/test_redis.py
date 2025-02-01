import pytest

from nqlstore.redis import Field, HashModel
from tests.utils import insert_test_data, load_fixture

_LIBRARY_DATA = load_fixture("libraries.json")


class Library(HashModel):
    address: str
    name: str = Field(index=True, full_text_search=True)

    @property
    def id(self):
        return self.pk


class Book(HashModel):
    title: str = Field(index=True)
    library_id: str

    @property
    def id(self):
        return self.pk


@pytest.mark.asyncio
async def test_find_native(redis_store):
    """Find should return the items that match the native filter"""
    inserted_libs, inserted_books = await insert_test_data(
        redis_store, library_model=Library, book_model=Book
    )
    address = "Hoima, Uganda"

    got = await redis_store.find(
        Library, (Library.address == address) | (Library.name.startswith("ba")), skip=1
    )
    expected = [
        v
        for v in inserted_libs
        if v.address == address or v.name.lower().startswith("ba")
    ][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_find_mongo_style(redis_store):
    """Find should return the items that match the mongodb-like filter"""
    inserted_libs, inserted_books = await insert_test_data(
        redis_store, library_model=Library, book_model=Book
    )

    address = "Hoima, Uganda"

    got = await redis_store.find(
        Library,
        nql_query={"$or": [{"address": {"$eq": address}}, {"name": {"$eq": "Bar"}}]},
        skip=1,
    )
    expected = [v for v in inserted_libs if v.address == address or v.name == "Bar"][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_find_hybrid(redis_store):
    """Find should return the items that match the mongodb-like filter AND the native filter"""
    inserted_libs, inserted_books = await insert_test_data(
        redis_store, library_model=Library, book_model=Book
    )

    address = "Hoima, Uganda"

    got = await redis_store.find(
        Library,
        (Library.name.startswith("ba")),
        nql_query={"address": {"$eq": address}},
        skip=1,
    )
    expected = [
        v
        for v in inserted_libs
        if v.address == address or v.name.lower().startswith("ba")
    ][1:]
    assert got == expected


@pytest.mark.asyncio
async def test_create(redis_store):
    """Create should add many items to the sql database"""
    await redis_store.register([Library, Book])
    got = await redis_store.insert(Library, _LIBRARY_DATA)
    got = [v.dict(exclude={"pk"}) for v in got]
    assert got == _LIBRARY_DATA


@pytest.mark.asyncio
async def test_update(redis_store):
    """Update should update the items that match the filter"""
    inserted_libs, _ = await insert_test_data(
        redis_store, library_model=Library, book_model=Book
    )
    updates = {"address": "some new address"}
    startswith_bu = lambda v: v.name.lower().startswith("bu")

    expected_data_in_db = [
        (record.model_copy(update=updates) if startswith_bu(record) else record)
        for record in inserted_libs
    ]
    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await redis_store.update(
        Library, Library.name.startswith("bu"), updates=updates
    )
    expected = list(filter(startswith_bu, expected_data_in_db))
    assert got == expected

    # all library data in database
    got = await redis_store.find(Library)
    assert _sort_by_name(got) == _sort_by_name(expected_data_in_db)


@pytest.mark.asyncio
async def test_delete(redis_store):
    """Delete should remove the items that match the filter"""
    inserted_libs, _ = await insert_test_data(
        redis_store, library_model=Library, book_model=Book
    )

    # in immediate response
    # NOTE: redis startswith/contains on single letters is not supported by redis
    got = await redis_store.delete(Library, Library.name.startswith("bu"))
    expected = [v for v in inserted_libs if v.name.lower().startswith("bu")]
    assert got == expected

    # all data in database
    got = await redis_store.find(Library)
    expected = [v for v in inserted_libs if not v.name.lower().startswith("bu")]
    assert _sort_by_name(got) == _sort_by_name(expected)


def _sort_by_name(libraries: list[Library]) -> list[Library]:
    """Sorts the given libraries by name

    Args:
        libraries: the libraries to sort

    Returns:
        the sorted libraries
    """
    return sorted(libraries, key=lambda v: v.name)
