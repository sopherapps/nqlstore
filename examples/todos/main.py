import os
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Query
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


app = FastAPI()


@app.get("/todos/")
async def search(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    q: str = Query(...),
):
    """Searches for todos by name"""
    pass


@app.get("/todos/{id}")
async def get_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    id: int | str,
):
    """Get todolist by id"""
    pass


@app.post("/todos/")
async def create_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    payload: TodoList,
):
    """Create a todolist"""
    pass


@app.put("/todos/{id}")
async def update_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    id: int | str,
    payload: TodoList,
):
    """Update a todolist"""
    pass


@app.delete("/todos/{id}")
async def delete_one(
    sql: _SqlStoreDep,
    redis: _RedisStoreDep,
    mongo: _MongoStoreDep,
    id: int | str,
    payload: TodoList,
):
    """Delete a todolist"""
    pass
