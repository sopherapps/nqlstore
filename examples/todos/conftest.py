"""Fixtures for tests"""

from typing import Any

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from main import MongoTodoList, RedisTodoList, SqlTodoList
from pytest_lazyfixture import lazy_fixture

from nqlstore import MongoStore, RedisStore, SQLStore

STORE_MODEL_PAIRS = [
    (lazy_fixture("sql_store"), SqlTodoList),
    (lazy_fixture("redis_store"), RedisTodoList),
    (lazy_fixture("mongo_store"), MongoTodoList),
]

STORE_MODEL_TODO_LISTS_TUPLES = [
    (lazy_fixture("sql_store"), SqlTodoList, lazy_fixture("sql_todolists")),
    (lazy_fixture("redis_store"), RedisTodoList, lazy_fixture("redis_todolists")),
    (lazy_fixture("mongo_store"), MongoTodoList, lazy_fixture("mongo_todolists")),
]

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
    from .main import MongoTodoList

    records = await mongo_store.insert(MongoTodoList, TODO_LISTS)
    yield records


@pytest_asyncio.fixture()
async def redis_todolists(redis_store: RedisStore):
    """A list of todolists in the redis store"""
    from .main import RedisTodoList

    records = await redis_store.insert(RedisTodoList, TODO_LISTS)
    yield records
