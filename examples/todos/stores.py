"""Module containing the registry of stores at runtime"""

import os

from models import (
    MongoTodo,
    MongoTodoList,
    RedisTodo,
    RedisTodoList,
    SqlTodo,
    SqlTodoList,
)

from nqlstore import MongoStore, RedisStore, SQLStore

_STORES: dict[str, MongoStore | RedisStore | SQLStore] = {}


async def get_redis_store() -> RedisStore | None:
    """Gets the redis store whose URL is specified in the environment"""
    global _STORES

    if redis_url := os.environ.get("REDIS_URL"):
        try:
            return _STORES[redis_url]
        except KeyError:
            store = RedisStore(uri=redis_url)
            await store.register([RedisTodoList, RedisTodo])
            _STORES[redis_url] = store
            return store


async def get_sql_store() -> SQLStore | None:
    """Gets the sql store whose URL is specified in the environment"""
    global _STORES

    if sql_url := os.environ.get("SQL_URL"):
        try:
            return _STORES[sql_url]
        except KeyError:
            store = SQLStore(uri=sql_url)
            await store.register([SqlTodoList, SqlTodo])
            _STORES[sql_url] = store
            return store


async def get_mongo_store() -> MongoStore | None:
    """Gets the mongo store whose URL and database are specified in the environment"""
    global _STORES

    if mongo_url := os.environ.get("MONGO_URL"):
        mongo_db = os.environ.get("MONGO_DB", "todos")
        mongo_full_url = f"{mongo_url}/{mongo_db}"

        try:
            return _STORES[mongo_full_url]
        except KeyError:
            store = MongoStore(uri=mongo_url, database=mongo_db)
            await store.register([MongoTodoList, MongoTodo])
            _STORES[mongo_full_url] = store
            return store


def clear_stores():
    """Clears the registry of stores

    Important for clean up
    """
    global _STORES
    _STORES.clear()
