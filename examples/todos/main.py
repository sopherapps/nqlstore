import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, status
from models import (
    MongoTodo,
    MongoTodoList,
    RedisTodo,
    RedisTodoList,
    SqlTodo,
    SqlTodoList,
)
from schemas import TodoList

from nqlstore import (
    MongoStore,
    RedisStore,
    SQLStore,
)

_SQL_STORE: SQLStore | None = None
_REDIS_STORE: RedisStore | None = None
_MONGO_STORE: MongoStore | None = None

# dependencies
_SqlStoreDep = Annotated[SQLStore | None, Depends(lambda: _SQL_STORE)]
_RedisStoreDep = Annotated[RedisStore | None, Depends(lambda: _REDIS_STORE)]
_MongoStoreDep = Annotated[MongoStore | None, Depends(lambda: _MONGO_STORE)]


# startup events
@asynccontextmanager
async def lifespan(app_: FastAPI):
    sql_url = os.environ.get("SQL_URL", "")
    redis_url = os.environ.get("REDIS_URL", "")
    mongo_url = os.environ.get("MONGO_URL", "")
    mongo_db = os.environ.get("MONGO_DB", "todos")

    if sql_url:
        global _SQL_STORE
        _SQL_STORE = SQLStore(uri=sql_url)
        await _SQL_STORE.register([SqlTodoList, SqlTodo])

    if redis_url:
        global _REDIS_STORE
        _REDIS_STORE = RedisStore(uri=redis_url)
        await _REDIS_STORE.register([RedisTodoList, RedisTodo])

    if mongo_url:
        global _MONGO_STORE
        _MONGO_STORE = MongoStore(uri=mongo_url, database=mongo_db)
        await _MONGO_STORE.register([MongoTodoList, MongoTodo])

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


@app.get("/todos/{id}")
async def get_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    id: int | str,
):
    """Get todolist by id"""
    results = []
    query = {"id": {"$eq": id}}

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

        return results[0]
    except Exception as exp:
        logging.error(exp)
        raise exp


@app.put("/todos/{id}")
async def update_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    id: int | str,
    payload: TodoList,
):
    """Update a todolist"""
    results = []
    query = {"id": {"$eq": id}}
    updates = payload.model_dump(exclude_unset=True)

    try:
        if sql:
            results += await sql.update(SqlTodoList, query=query, updates=updates)

        if redis:
            results += await redis.update(RedisTodoList, query=query, updates=updates)

        if mongo:
            results += await mongo.update(MongoTodoList, query=query, updates=updates)
    except Exception as exp:
        logging.error(exp)
        raise exp

    try:
        return results[0]
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


@app.delete("/todos/{id}")
async def delete_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    id: int | str,
):
    """Delete a todolist"""
    results = []
    query = {"id": {"$eq": id}}

    try:
        if sql:
            results += await sql.delete(SqlTodoList, query=query)

        if redis:
            results += await redis.find(RedisTodoList, query=query)

        if mongo:
            results += await mongo.find(MongoTodoList, query=query)
    except Exception as exp:
        logging.error(exp)
        raise exp

    try:
        return results[0]
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
