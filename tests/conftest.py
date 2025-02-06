from typing import Generic, TypeVar

import pytest
import pytest_asyncio
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

from .utils import get_regex_test_params, insert_test_data, is_lib_installed

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


if is_lib_installed("sqlmodel"):
    SqlLibrary = SQLModel("SqlLibrary", Library)
    SqlBook = SQLModel("SqlBook", Book[int])
else:
    SqlLibrary = Library
    SqlBook = Book[int]

if is_lib_installed("beanie"):
    MongoLibrary = MongoModel("MongoLibrary", Library)
    MongoBook = MongoModel("MongoBook", Book[PydanticObjectId])
else:
    MongoLibrary = Library
    MongoBook = Book[str]

if is_lib_installed("redis_om"):
    RedisLibrary = HashModel("RedisLibrary", Library)
    RedisBook = HashModel("RedisBook", Book[str])
else:
    RedisLibrary = Library
    RedisBook = Book[str]


@pytest.fixture
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def sql_store():
    """The sql store stored in memory"""
    store = SQLStore(uri="sqlite+aiosqlite:///:memory:")
    yield store


@pytest.fixture
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
def mongo_store():
    """The mongodb store"""
    import pymongo

    store = MongoStore(uri="mongodb://localhost:27017", database="testing")
    yield store

    # clean up after the test
    client = pymongo.MongoClient("mongodb://localhost:27017")
    client.drop_database("testing")


@pytest.fixture
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def redis_store():
    """The redis store"""
    import redis

    store = RedisStore(uri="redis://localhost:6379/0")
    yield store

    # clean up after the test
    client = redis.Redis("localhost", 6379, 0)
    client.flushall()


@pytest_asyncio.fixture()
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
async def inserted_redis_libs(redis_store):
    """The libraries inserted in the redis store"""
    inserted_libs, _ = await insert_test_data(
        redis_store, library_model=RedisLibrary, book_model=RedisBook
    )
    yield inserted_libs


@pytest.fixture
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def regex_params_redis(inserted_redis_libs):
    """The regex test params for redis"""
    yield get_regex_test_params(inserted_redis_libs)


@pytest_asyncio.fixture()
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
async def inserted_mongo_libs(mongo_store):
    """The libraries inserted in the mongodb store"""
    inserted_libs, _ = await insert_test_data(
        mongo_store, library_model=MongoLibrary, book_model=MongoBook
    )
    yield inserted_libs


@pytest.fixture
@pytest.mark.skipif(not is_lib_installed("beanie"), reason="Requires beanie.")
def regex_params_mongo(inserted_mongo_libs):
    """The regex test params for mongo"""
    yield get_regex_test_params(inserted_mongo_libs)


@pytest_asyncio.fixture()
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def inserted_sql_libs(sql_store):
    """The libraries inserted in the sql store"""
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=SqlLibrary, book_model=SqlBook
    )
    yield inserted_libs


@pytest.fixture
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def regex_params_sql(inserted_sql_libs):
    """The regex test params for sql"""
    yield get_regex_test_params(inserted_sql_libs)


@pytest_asyncio.fixture()
@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
async def sql_qparser():
    """The default query parser for sql"""
    qparser = QueryParser()
    sql_store = SQLStore(uri="sqlite+aiosqlite:///:memory:", parser=qparser)
    await sql_store.register([SqlLibrary, SqlBook])
    yield qparser


@pytest_asyncio.fixture()
@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
async def redis_qparser():
    """The default query parser for redis"""
    import redis

    qparser = QueryParser()
    redis_store = RedisStore(uri="redis://localhost:6379/0", parser=qparser)
    await redis_store.register([RedisLibrary, RedisBook])

    yield qparser

    # clean up after the test
    client = redis.Redis("localhost", 6379, 0)
    client.flushall()
