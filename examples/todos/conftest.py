"""Fixtures for tests"""

import os
from typing import Any

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from models import (
    MongoTodo,
    MongoTodoList,
    RedisTodo,
    RedisTodoList,
    SqlTodo,
    SqlTodoList,
)

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

_SQL_DB = "test.db"
_SQL_URL = f"sqlite+aiosqlite:///{_SQL_DB}"
_REDIS_URL = "redis://localhost:6379/0"
_MONGO_URL = "mongodb://localhost:27017"
_MONGO_DB = "testing"


@pytest.fixture
def client_with_sql():
    """The fastapi test client when SQL is enabled"""
    _reset_env()
    os.environ["SQL_URL"] = _SQL_URL

    from main import app

    yield TestClient(app)
    _reset_env()


@pytest_asyncio.fixture
async def client_with_redis():
    """The fastapi test client when redis is enabled"""
    _reset_env()
    os.environ["REDIS_URL"] = _REDIS_URL

    from main import app

    yield TestClient(app)
    _reset_env()


@pytest.fixture
def client_with_mongo():
    """The fastapi test client when mongodb is enabled"""
    _reset_env()

    os.environ["MONGO_URL"] = _MONGO_URL
    os.environ["MONGO_DB"] = _MONGO_DB

    from main import app

    yield TestClient(app)
    _reset_env()


@pytest_asyncio.fixture()
async def sql_store():
    """The sql store stored in memory"""
    store = SQLStore(uri=_SQL_URL)

    await store.register([SqlTodoList, SqlTodo])
    yield store

    # clean up
    os.remove(_SQL_DB)


@pytest_asyncio.fixture()
async def mongo_store():
    """The mongodb store. Requires a running instance of mongodb"""
    import pymongo

    mongo_store = MongoStore(uri=_MONGO_URL, database=_MONGO_DB)
    await mongo_store.register([MongoTodoList, MongoTodo])

    yield mongo_store

    # clean up
    client = pymongo.MongoClient(_MONGO_URL)  # type: ignore
    client.drop_database(_MONGO_DB)


@pytest_asyncio.fixture
async def redis_store():
    """The redis store. Requires a running instance of redis stack"""
    import redis

    store = RedisStore(_REDIS_URL)
    await store.register([RedisTodoList, RedisTodo])

    yield store

    # clean up
    client = redis.Redis("localhost", 6379, 0)
    client.flushall()


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


def _reset_env():
    """Resets the environment variables available to the app"""
    os.environ["SQL_URL"] = ""
    os.environ["REDIS_URL"] = ""
    os.environ["MONGO_URL"] = ""
    os.environ["MONGO_DB"] = "testing"
