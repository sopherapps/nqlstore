import pymongo
import pytest
import redis

from nqlstore.mongo import MongoStore
from nqlstore.redis import RedisStore
from nqlstore.sql import SQLStore


@pytest.fixture
def sql_store():
    """The sql store stored in memory"""
    store = SQLStore(uri="sqlite+aiosqlite:///:memory:")
    yield store


@pytest.fixture
def mongo_store():
    """The mongodb store"""
    store = MongoStore(uri="mongodb://localhost:27017", database="testing")
    yield store

    # clean up after the test
    client = pymongo.MongoClient("mongodb://localhost:27017")
    client.drop_database("testing")


@pytest.fixture
def redis_store():
    """The redis store"""
    store = RedisStore(uri="redis://localhost:6379/0")
    yield store

    # clean up after the test
    client = redis.Redis("localhost", 6379, 0)
    client.flushall()
