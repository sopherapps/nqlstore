"""Fixtures for tests"""

import asyncio
import os
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from main import get_redis_store
from models import MongoTodo, MongoTodoList, RedisTodo, RedisTodoList

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
def client_with_sql():
    """The fastapi test client when SQL is enabled"""
    _reset_env()
    os.environ["SQL_URL"] = "sqlite+aiosqlite:///test.db"

    from main import app

    yield TestClient(app)
    _reset_env()

    os.remove("test.db")


@pytest_asyncio.fixture()
async def client_with_redis(event_loop):
    """The fastapi test client when redis is enabled"""
    _reset_env()
    redis_url = "redis://localhost:6379/0"
    os.environ["REDIS_URL"] = redis_url

    import redis
    from main import app

    yield TestClient(app)
    _reset_env()

    client = redis.Redis("localhost", 6379, 0)
    client.flushall()


@pytest.fixture
def client_with_mongo(event_loop):
    """The fastapi test client when mongodb is enabled"""
    _reset_env()

    mongo_url = "mongodb://localhost:27017"
    mongo_db = "testing"
    os.environ["MONGO_URL"] = mongo_url
    os.environ["MONGO_DB"] = mongo_db

    import pymongo
    from main import app

    yield TestClient(app)
    _reset_env()

    client = pymongo.MongoClient(mongo_url)  # type: ignore
    client.drop_database(mongo_db)


@pytest.fixture
def sql_store(client_with_sql: TestClient):
    """The sql store stored in memory"""
    # using context manager to ensure on_startup runs
    with client_with_sql as client:
        yield client.app.state.sql


@pytest_asyncio.fixture()
async def mongo_store(client_with_mongo: TestClient):
    """The mongodb store. Requires a running instance of mongodb"""
    # using context manager to ensure on_startup runs
    with client_with_mongo as client:
        mongo_url = "mongodb://localhost:27017"
        mongo_db = "testing"

        mongo_store = MongoStore(uri=mongo_url, database=mongo_db)
        await mongo_store.register([MongoTodoList, MongoTodo])
        # client.app.state.mongo = mongo_store

        yield mongo_store


@pytest_asyncio.fixture
async def redis_store(client_with_redis: TestClient):
    """The redis store. Requires a running instance of redis stack"""
    # using context manager to ensure on_startup runs
    # with client_with_redis as client:
    redis_url = "redis://localhost:6379/0"

    store = RedisStore(redis_url)
    await store.register([RedisTodoList, RedisTodo])
    #
    # client.app.state.redis = store
    # yield client.app.state.redis
    yield store


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
async def redis_todolists(client_with_redis: TestClient):
    """A list of todolists in the redis store"""
    from main import RedisTodoList

    with client_with_redis as client:

        records = await client.app.state.redis.insert(RedisTodoList, TODO_LISTS)
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


def _reset_env():
    """Resets the environment variables available to the app"""
    os.environ["SQL_URL"] = ""
    os.environ["REDIS_URL"] = ""
    os.environ["MONGO_URL"] = ""
    os.environ["MONGO_DB"] = "testing"
