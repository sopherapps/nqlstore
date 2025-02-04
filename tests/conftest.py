import pymongo
import pytest
import pytest_asyncio
import redis

from nqlstore.mongo import Document, Indexed, MongoStore, PydanticObjectId
from nqlstore.query.parsers import QueryParser
from nqlstore.redis import Field as RedisField
from nqlstore.redis import HashModel, RedisStore
from nqlstore.sql import Field as SqlField
from nqlstore.sql import SQLModel, SQLStore

from .utils import insert_test_data


class MongoLibrary(Document):
    address: str
    name: str

    class Settings:
        name = "libraries"


class MongoBook(Document):
    title: Indexed(str)
    library_id: PydanticObjectId

    class Settings:
        name = "books"


class RedisLibrary(HashModel):
    address: str = RedisField(index=True, full_text_search=True)
    name: str = RedisField(index=True, full_text_search=True)

    @property
    def id(self):
        return self.pk


class RedisBook(HashModel):
    title: str = RedisField(index=True)
    library_id: str

    @property
    def id(self):
        return self.pk


class SqlLibrary(SQLModel, table=True):
    id: int | None = SqlField(default=None, primary_key=True)
    address: str
    name: str


class SqlBook(SQLModel, table=True):
    id: int | None = SqlField(default=None, primary_key=True)
    title: str
    library_id: int = SqlField(default=None, foreign_key="sqllibrary.id")


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


@pytest_asyncio.fixture()
async def inserted_mongo_libs(mongo_store):
    """The libraries inserted in the mongodb store"""
    inserted_libs, _ = await insert_test_data(
        mongo_store, library_model=MongoLibrary, book_model=MongoBook
    )
    yield inserted_libs


@pytest_asyncio.fixture()
async def inserted_sql_libs(sql_store):
    """The libraries inserted in the sql store"""
    inserted_libs, _ = await insert_test_data(
        sql_store, library_model=SqlLibrary, book_model=SqlBook
    )
    yield inserted_libs


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
