from typing import Generic, TypeVar

import pymongo
import pytest
import pytest_asyncio
import redis
from pydantic import BaseModel

from nqlstore import (
    Field,
    HashModel,
    MongoModel,
    MongoStore,
    PydanticObjectId,
    RedisStore,
    SQLModel,
    SQLStore,
)
from nqlstore.query.parsers import QueryParser

from .utils import get_regex_test_params, insert_test_data

_T = TypeVar("_T")


class Library(BaseModel):
    address: str = Field(index=True, full_text_search=True)
    name: str = Field(index=True, full_text_search=True)

    class Settings:
        name = "libraries"


class Book(BaseModel, Generic[_T]):
    title: str = Field(index=True)
    library_id: _T | None = Field(default=None, foreign_key="sqllibrary.id")

    class Settings:
        name = "books"


MongoLibrary = MongoModel("MongoLibrary", Library)
MongoBook = MongoModel("MongoBook", Book[PydanticObjectId])
RedisLibrary = HashModel("RedisLibrary", Library)
RedisBook = HashModel("RedisBook", Book[str])
SqlLibrary = SQLModel("SqlLibrary", Library)
SqlBook = SQLModel("SqlBook", Book[int])


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


@pytest_asyncio.fixture()
async def inserted_redis_libs(redis_store):
    """The libraries inserted in the redis store"""
    inserted_libs, _ = await insert_test_data(
        redis_store, library_model=RedisLibrary, book_model=RedisBook
    )
    yield inserted_libs


@pytest.fixture
def regex_params_redis(inserted_redis_libs):
    """The regex test params for redis"""
    yield get_regex_test_params(inserted_redis_libs)


@pytest_asyncio.fixture()
async def inserted_mongo_libs(mongo_store):
    """The libraries inserted in the mongodb store"""
    inserted_libs, _ = await insert_test_data(
        mongo_store, library_model=MongoLibrary, book_model=MongoBook
    )
    yield inserted_libs


@pytest.fixture
def regex_params_mongo(inserted_mongo_libs):
    """The regex test params for mongo"""
    yield get_regex_test_params(inserted_mongo_libs)


@pytest_asyncio.fixture()
async def inserted_sql_libs(sql_store):
    """The libraries inserted in the sql store"""
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=SqlLibrary, book_model=SqlBook
    )
    yield inserted_libs


@pytest.fixture
def regex_params_sql(inserted_sql_libs):
    """The regex test params for sql"""
    yield get_regex_test_params(inserted_sql_libs)


@pytest_asyncio.fixture()
async def query_parser():
    """The default query parser"""
    qparser = QueryParser()
    redis_store = RedisStore(uri="redis://localhost:6379/0", parser=qparser)
    sql_store = SQLStore(uri="sqlite+aiosqlite:///:memory:", parser=qparser)
    await redis_store.register([RedisLibrary, RedisBook])
    await sql_store.register([SqlLibrary, SqlBook])

    yield qparser

    # clean up after the test
    client = redis.Redis("localhost", 6379, 0)
    client.flushall()
