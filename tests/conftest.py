import pytest
import pytest_asyncio
from pydantic import BaseModel

from nqlstore import (
    EmbeddedJsonModel,
    EmbeddedMongoModel,
    Field,
    JsonModel,
    MongoModel,
    MongoStore,
    RedisStore,
    Relationship,
    SQLModel,
    SQLStore,
)
from nqlstore.query.parsers import QueryParser

from .utils import get_regex_test_params, insert_test_data, is_lib_installed


class Library(BaseModel):
    address: str = Field(index=True, full_text_search=True)
    name: str = Field(index=True, full_text_search=True)
    books: list["Book"] = Relationship(back_populates="library", default=[])

    class Settings:
        name = "libraries"


class Book(BaseModel):
    title: str = Field(index=True)
    library_id: int | None = Field(
        default=None,
        foreign_key="sqllibrary.id",
        disable_on_redis=True,
        disable_on_mongo=True,
    )
    library: Library | None = Relationship(
        back_populates="books", disable_on_redis=True, disable_on_mongo=True
    )


# default models
SqlLibrary = Library
SqlBook = Book
MongoLibrary = Library
MongoBook = Book
RedisLibrary = Library
RedisBook = Book

if is_lib_installed("sqlmodel"):
    SqlLibrary = SQLModel(
        "SqlLibrary", Library, relationships={"books": list["SqlBook"]}
    )
    SqlBook = SQLModel("SqlBook", Book, relationships={"library": SqlLibrary | None})

if is_lib_installed("beanie"):
    MongoBook = EmbeddedMongoModel("MongoBook", Book)
    MongoLibrary = MongoModel(
        "MongoLibrary", Library, embedded_models={"books": list[MongoBook]}
    )

if is_lib_installed("redis_om"):
    RedisBook = EmbeddedJsonModel("RedisBook", Book)
    RedisLibrary = JsonModel(
        "RedisLibrary", Library, embedded_models={"books": list[RedisBook]}
    )


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
    inserted_libs = await insert_test_data(
        redis_store,
        library_model=RedisLibrary,
        book_model=RedisBook,
    )
    # sanity check
    all_books = [bk for lib in inserted_libs for bk in lib.books]
    assert all_books != []

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
    inserted_libs = await insert_test_data(
        mongo_store, library_model=MongoLibrary, book_model=MongoBook
    )
    # sanity check
    all_books = [bk for lib in inserted_libs for bk in lib.books]
    assert all_books != []

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
    inserted_libs = await insert_test_data(
        sql_store, library_model=SqlLibrary, book_model=SqlBook
    )
    # sanity check
    all_books = [bk for lib in inserted_libs for bk in lib.books]
    assert all_books != []

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
