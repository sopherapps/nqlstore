import pymongo
import pytest

from nqlstore.mongo import MongoStore
from nqlstore.sql import SQLStore


@pytest.fixture
def sql_store():
    """The sql store stored in memory"""
    store = SQLStore(uri="sqlite+aiosqlite:///:memory:")
    yield store


@pytest.fixture
def mongo_store():
    """The mongodb store stored in memory"""
    store = MongoStore(uri="mongodb://localhost:27017", database="testing")
    yield store

    client = pymongo.MongoClient("mongodb://localhost:27017")
    client.drop_database("testing")
