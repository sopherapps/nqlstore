"""Fixtures for tests"""

from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from nqlstore import MongoStore, RedisStore, SQLStore

TODO_LISTS: list[dict[str, Any]] = [
    {"name": "School Work"},
    {
        "name": "Home",
        "todos": [
            {"title": "Make my bed"},
            {"title": "Cook supper"},
        ],
    },
    {"name": "Boo", "todos": [{"title": "Talk endlessly till daybreak"}]},
]


@pytest.fixture
def sql_store():
    """The sql store stored in memory"""
    store = SQLStore(uri="sqlite+aiosqlite:///:memory:")
    yield store


@pytest.fixture
def mongo_store():
    """The mongodb store. Requires a running instance of mongodb"""
    import pymongo

    store = MongoStore(uri="mongodb://localhost:27017", database="testing")
    yield store

    # clean up after the test
    client = pymongo.MongoClient("mongodb://localhost:27017")  # type: ignore
    client.drop_database("testing")


@pytest.fixture
def redis_store():
    """The redis store. Requires a running instance of redis stack"""
    import redis

    store = RedisStore(uri="redis://localhost:6379/0")
    yield store

    # clean up after the test
    client = redis.Redis("localhost", 6379, 0)
    client.flushall()


@pytest.fixture
def client():
    """The fastapi test client"""
    from main import app

    yield TestClient(app)


@pytest_asyncio.fixture()
async def sql_todolists(sql_store: SQLStore):
    """A list of todolists in the sql store"""
    from main import SqlTodoList

    records = await sql_store.insert(SqlTodoList, TODO_LISTS)
    yield records


@pytest_asyncio.fixture()
async def mongo_todolists(mongo_store: MongoStore):
    """A list of todolists in the mongo store"""
    from main import MongoTodoList

    records = await mongo_store.insert(MongoTodoList, TODO_LISTS)
    yield records


@pytest_asyncio.fixture()
async def redis_todolists(redis_store: RedisStore):
    """A list of todolists in the redis store"""
    from main import RedisTodoList

    records = await redis_store.insert(RedisTodoList, TODO_LISTS)
    yield records


@dataclass(frozen=True)
class LazyFixture:
    """A fixture to be resolved lazily."""

    name: str


def lazy_fixture(name: str) -> LazyFixture:
    """Create a lazy fixture."""
    return LazyFixture(name)


@pytest.hookimpl(tryfirst=True)
def pytest_fixture_setup(
    fixturedef: pytest.FixtureDef,
    request: pytest.FixtureRequest,
) -> object | None:
    """Pytest hook to load lazy fixtures during setup

    https://docs.pytest.org/en/latest/reference/reference.html#pytest.hookspec.pytest_fixture_setup
    Stops at first non-None result

    Args:
        fixturedef: fixture definition object.
        request: fixture request object.

    Returns:
        fixture value or None.
    """
    param = getattr(request, "param", None)
    if isinstance(param, LazyFixture):
        request.param = request.getfixturevalue(param.name)
    return None
