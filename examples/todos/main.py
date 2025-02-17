import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated

from beanie import PydanticObjectId
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from models import (
    MongoTodo,
    MongoTodoList,
    RedisTodo,
    RedisTodoList,
    SqlTodo,
    SqlTodoList,
)
from schemas import TodoList

from nqlstore import MongoStore, RedisStore, SQLStore


# dependencies
def get_redis_store(req: Request) -> RedisStore | None:
    """Dependency injector for the redis store"""
    try:
        return req.app.state.redis
    except (KeyError, AttributeError):
        return None


def get_sql_store(req: Request) -> SQLStore | None:
    """Dependency injector for the sql store"""
    try:
        return req.app.state.sql
    except (KeyError, AttributeError):
        return None


def get_mongo_store(req: Request) -> MongoStore | None:
    """Dependency injector for the mongo store"""
    try:
        return req.app.state.mongo
    except (KeyError, AttributeError):
        return None


_SqlStoreDep = Annotated[SQLStore | None, Depends(get_sql_store)]
_RedisStoreDep = Annotated[RedisStore | None, Depends(get_redis_store)]
_MongoStoreDep = Annotated[MongoStore | None, Depends(get_mongo_store)]


# startup events
@asynccontextmanager
async def lifespan(app_: FastAPI):
    sql_url = os.environ.get("SQL_URL", "")
    redis_url = os.environ.get("REDIS_URL", "")
    mongo_url = os.environ.get("MONGO_URL", "")
    mongo_db = os.environ.get("MONGO_DB", "todos")

    if sql_url:
        sql_store = SQLStore(uri=sql_url)
        await sql_store.register([SqlTodoList, SqlTodo])
        app_.state.sql = sql_store

    if redis_url:
        redis_store = RedisStore(uri=redis_url)
        await redis_store.register([RedisTodoList, RedisTodo])
        app_.state.redis = redis_store

    if mongo_url:
        mongo_store = MongoStore(uri=mongo_url, database=mongo_db)
        await mongo_store.register([MongoTodoList, MongoTodo])
        app_.state.mongo = mongo_store

    yield


app = FastAPI(lifespan=lifespan)


@app.get("/todos")
async def search(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    q: str = Query(...),
):
    """Searches for todos by name"""

    results = []
    query = {"name": {"$in": q}}

    try:
        if sql:
            results += await sql.find(SqlTodoList, query=query)

        if redis:
            results += await redis.find(RedisTodoList, query=query)

        if mongo:
            results += await mongo.find(MongoTodoList, query=query)
    except Exception as exp:
        logging.error(exp)
        raise exp

    return results


@app.get("/todos/{id_}")
async def get_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    id_: int | str,
):
    """Get todolist by id"""
    results = []
    query = {"id": {"$eq": id_}}

    try:
        if sql:
            results += await sql.find(SqlTodoList, query=query, limit=1)

        if redis:
            results += await redis.find(RedisTodoList, query=query, limit=1)

        if mongo:
            results += await mongo.find(MongoTodoList, query=query, limit=1)
    except Exception as exp:
        logging.error(exp)
        raise exp

    try:
        return results[0]
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


@app.post("/todos")
async def create_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    payload: TodoList,
):
    """Create a todolist"""
    results = []
    payload_dict = payload.model_dump(exclude_unset=True)

    try:
        if sql:
            results += await sql.insert(SqlTodoList, [payload_dict])

        if redis:
            results += await redis.insert(RedisTodoList, [payload_dict])

        if mongo:
            results += await mongo.insert(MongoTodoList, [payload_dict])

        result = results[0].model_dump(mode="json")
        return result
    except Exception as exp:
        logging.error(exp)
        raise exp


@app.put("/todos/{id_}")
async def update_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    id_: int | str,
    payload: TodoList,
):
    """Update a todolist"""
    results = []
    query = {"id": {"$eq": id_}}
    updates = payload.model_dump(exclude_unset=True)

    try:
        if sql:
            results += await sql.update(SqlTodoList, query=query, updates=updates)

        if redis:
            results += await redis.update(RedisTodoList, query=query, updates=updates)

        if mongo:
            results += await mongo.update(
                MongoTodoList,
                query={"_id": {"$eq": PydanticObjectId(id_)}},
                updates=updates,
            )
    except Exception as exp:
        logging.error(exp)
        raise exp

    try:
        return results[0].model_dump(mode="json")
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


@app.delete("/todos/{id_}")
async def delete_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    id_: int | str,
):
    """Delete a todolist"""
    results = []
    query = {"id": {"$eq": id_}}

    try:
        if sql:
            results += await sql.delete(SqlTodoList, query=query)

        if redis:
            results += await redis.delete(RedisTodoList, query=query)

        if mongo:
            results += await mongo.delete(
                MongoTodoList, query={"_id": {"$eq": PydanticObjectId(id_)}}
            )
    except Exception as exp:
        logging.error(exp)
        raise exp

    try:
        return results[0].model_dump(mode="json")
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
