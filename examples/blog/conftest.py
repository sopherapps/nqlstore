"""Fixtures for tests"""

import os
from typing import Any

import pytest
import pytest_asyncio
import pytest_mock
from fastapi.testclient import TestClient
from models import (
    MongoAuthor,
    MongoComment,
    MongoInternalAuthor,
    MongoPost,
    MongoTag,
    RedisAuthor,
    RedisComment,
    RedisInternalAuthor,
    RedisPost,
    RedisTag,
    SqlComment,
    SqlInternalAuthor,
    SqlPost,
    SqlTag,
    SqlTagLink,
)

from nqlstore import MongoStore, RedisStore, SQLStore

POST_LISTS: list[dict[str, Any]] = [
    {
        "title": "School Work",
        "content": "foo bar man the stuff",
    },
    {
        "title": "Home",
        "tags": [
            {"title": "home"},
            {"title": "art"},
        ],
    },
    {"title": "Boo", "content": "some random stuff", "tags": [{"title": "random"}]},
]

COMMENT_LIST: list[dict[str, Any]] = [
    {
        "content": "Fake comment",
    },
    {
        "content": "Just wondering, who wrote this?",
    },
    {
        "content": "Mann, this is off the charts!",
    },
    {
        "content": "Woo hoo",
    },
    {
        "content": "Not cool. Not cool at all.",
    },
]

AUTHOR: dict[str, Any] = {
    "name": "John Doe",
    "email": "johndoe@example.com",
    "password": "password123",
}

_SQL_DB = "test.db"
_SQL_URL = f"sqlite+aiosqlite:///{_SQL_DB}"
_REDIS_URL = "redis://localhost:6379/0"
_MONGO_URL = "mongodb://localhost:27017"
_MONGO_DB = "testing"
ACCESS_TOKEN = "some-token"


@pytest.fixture
def mocked_auth(mocker: pytest_mock.MockerFixture):
    """Mocks the auth to always return the AUTHOR as valid"""
    mocker.patch("jwt.encode", return_value=ACCESS_TOKEN)
    mocker.patch("jwt.decode", return_value={"sub": AUTHOR["email"]})
    mocker.patch("auth.pwd_context.verify", return_value=True)
    yield


@pytest.fixture
def client_with_sql(mocked_auth):
    """The fastapi test client when SQL is enabled"""
    _reset_env()
    os.environ["SQL_URL"] = _SQL_URL

    from main import app

    yield TestClient(app)
    _reset_env()


@pytest_asyncio.fixture
async def client_with_redis(mocked_auth):
    """The fastapi test client when redis is enabled"""
    _reset_env()
    os.environ["REDIS_URL"] = _REDIS_URL

    from main import app

    yield TestClient(app)
    _reset_env()


@pytest.fixture
def client_with_mongo(mocked_auth):
    """The fastapi test client when mongodb is enabled"""
    _reset_env()

    os.environ["MONGO_URL"] = _MONGO_URL
    os.environ["MONGO_DB"] = _MONGO_DB

    from main import app

    yield TestClient(app)
    _reset_env()


@pytest_asyncio.fixture()
async def sql_store(mocked_auth):
    """The sql store stored in memory"""
    store = SQLStore(uri=_SQL_URL)

    await store.register(
        [
            # SqlAuthor,
            SqlTag,
            SqlTagLink,
            SqlPost,
            SqlComment,
            SqlInternalAuthor,
        ]
    )
    # insert default user
    await store.insert(SqlInternalAuthor, [AUTHOR])
    yield store

    # clean up
    os.remove(_SQL_DB)


@pytest_asyncio.fixture()
async def mongo_store(mocked_auth):
    """The mongodb store. Requires a running instance of mongodb"""
    import pymongo

    store = MongoStore(uri=_MONGO_URL, database=_MONGO_DB)
    await store.register(
        [
            MongoAuthor,
            MongoTag,
            MongoPost,
            MongoComment,
            MongoInternalAuthor,
        ]
    )

    # insert default user
    await store.insert(MongoInternalAuthor, [AUTHOR])

    yield store

    # clean up
    client = pymongo.MongoClient(_MONGO_URL)  # type: ignore
    client.drop_database(_MONGO_DB)


@pytest_asyncio.fixture
async def redis_store(mocked_auth):
    """The redis store. Requires a running instance of redis stack"""
    import redis

    store = RedisStore(_REDIS_URL)
    await store.register(
        [
            RedisAuthor,
            RedisTag,
            RedisPost,
            RedisComment,
            RedisInternalAuthor,
        ]
    )
    # insert default user
    await store.insert(RedisInternalAuthor, [AUTHOR])

    yield store

    # clean up
    client = redis.Redis("localhost", 6379, 0)
    client.flushall()


@pytest_asyncio.fixture()
async def sql_posts(sql_store: SQLStore):
    """A list of posts in the sql store"""
    records = await sql_store.insert(SqlPost, POST_LISTS)
    yield records


@pytest_asyncio.fixture()
async def mongo_posts(mongo_store: MongoStore):
    """A list of posts in the mongo store"""

    records = await mongo_store.insert(MongoPost, POST_LISTS)
    yield records


@pytest_asyncio.fixture()
async def redis_posts(redis_store: RedisStore):
    """A list of posts in the redis store"""
    records = await redis_store.insert(RedisPost, POST_LISTS)
    yield records


def _reset_env():
    """Resets the environment variables available to the app"""
    os.environ["SQL_URL"] = ""
    os.environ["REDIS_URL"] = ""
    os.environ["MONGO_URL"] = ""
    os.environ["MONGO_DB"] = "testing"
    os.environ["JWT_SECRET"] = (
        "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
    )
