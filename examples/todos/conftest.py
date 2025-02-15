"""Fixtures for tests"""

import os
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
    sql_url = "sqlite+aiosqlite:///:memory:"
    os.environ["SQL_URL"] = sql_url

    store = SQLStore(uri=sql_url)
    yield store

    # cleanup
    os.environ["SQL_URL"] = ""


@pytest.fixture
def mongo_store():
    """The mongodb store. Requires a running instance of mongodb"""
    import pymongo

    mongo_url = "mongodb://localhost:27017"
    mongo_db = "testing"
    os.environ["MONGO_URL"] = mongo_url
    os.environ["MONGO_DB"] = mongo_db

    store = MongoStore(uri=mongo_url, database=mongo_db)
    yield store

    # clean up after the test
    client = pymongo.MongoClient("mongodb://localhost:27017")  # type: ignore
    client.drop_database("testing")
    os.environ["MONGO_URL"] = ""


@pytest.fixture
def redis_store():
    """The redis store. Requires a running instance of redis stack"""
    import redis

    redis_url = "redis://localhost:6379/0"
    os.environ["REDIS_URL"] = redis_url

    store = RedisStore(uri=redis_url)
    yield store

    # clean up after the test
    client = redis.Redis("localhost", 6379, 0)
    client.flushall()
    os.environ["REDIS_URL"] = ""


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
