import logging
from contextlib import asynccontextmanager
from typing import Annotated

from beanie import PydanticObjectId
from fastapi import Depends, FastAPI, HTTPException, Query, status
from models import MongoTodoList, RedisTodoList, SqlTodoList
from schemas import TodoList
from stores import clear_stores, get_mongo_store, get_redis_store, get_sql_store

from nqlstore import MongoStore, RedisStore, SQLStore

_SqlStoreDep = Annotated[SQLStore | None, Depends(get_sql_store)]
_RedisStoreDep = Annotated[RedisStore | None, Depends(get_redis_store)]
_MongoStoreDep = Annotated[MongoStore | None, Depends(get_mongo_store)]


@asynccontextmanager
async def lifespan(app_: FastAPI):
    clear_stores()
    yield
    clear_stores()


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
    query = {"name": {"$regex": f".*{q}.*", "$options": "i"}}

    try:
        if sql:
            results += await sql.find(SqlTodoList, query=query)

        if redis:
            # redis's regex search is not mature so we use its full text search
            results += await redis.find(RedisTodoList, (RedisTodoList.name % f"*{q}*"))

        if mongo:
            results += await mongo.find(MongoTodoList, query=query)
    except Exception as exp:
        logging.error(exp)
        raise exp

    return [item.model_dump(mode="json") for item in results]


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
            results += await mongo.find(
                MongoTodoList, query={"_id": {"$eq": PydanticObjectId(id_)}}, limit=1
            )
    except Exception as exp:
        logging.error(exp)
        raise exp

    try:
        return results[0].model_dump(mode="json")
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
