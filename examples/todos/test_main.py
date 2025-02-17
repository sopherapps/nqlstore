from typing import Any

import pytest
from bson import ObjectId
from conftest import TODO_LISTS, lazy_fixture
from fastapi.testclient import TestClient
from main import MongoTodoList, RedisTodoList, SqlTodoList

from nqlstore import MongoStore, RedisStore, SQLStore
from nqlstore._base import BaseModel, BaseStore

STORE_MODEL_TUPLES = [
    (lazy_fixture("client_with_sql"), lazy_fixture("sql_store"), SqlTodoList),
    (lazy_fixture("client_with_redis"), lazy_fixture("redis_store"), RedisTodoList),
    (lazy_fixture("client_with_mongo"), lazy_fixture("mongo_store"), MongoTodoList),
]

STORE_MODEL_TODO_LISTS_TUPLES = [
    (
        lazy_fixture("client_with_sql"),
        lazy_fixture("sql_store"),
        SqlTodoList,
        lazy_fixture("sql_todolists"),
    ),
    (
        lazy_fixture("client_with_redis"),
        lazy_fixture("redis_store"),
        RedisTodoList,
        lazy_fixture("redis_todolists"),
    ),
    (
        lazy_fixture("client_with_mongo"),
        lazy_fixture("mongo_store"),
        MongoTodoList,
        lazy_fixture("mongo_todolists"),
    ),
]

_PUT_DEL_GET_PARAMS = [
    (client, store, model, todolists, index)
    for client, store, model, todolists in STORE_MODEL_TODO_LISTS_TUPLES
    for index in range(len(TODO_LISTS))
]
_SEARCH_PARAMS = [
    (client, store, model, todolist, q)
    for client, store, model in STORE_MODEL_TUPLES
    for todolist in TODO_LISTS
    for q in ["ho", "oo", "work"]
]


@pytest.mark.asyncio
@pytest.mark.parametrize("todolist", TODO_LISTS)
async def test_create_sql_todolist(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    todolist: dict,
):
    """POST to /todos creates a todolist in sql and returns it"""
    response = client_with_sql.post("/todos", json=todolist)

    got = response.json()
    todolist_id = got["id"]
    raw_todos = todolist.get("todos", [])
    resp_todos = got["todos"]
    expected = {
        "id": todolist_id,
        "name": todolist["name"],
        "todos": [
            {
                **raw,
                "is_complete": "0",
                "id": resp["id"],
                "parent_id": todolist_id,
            }
            for raw, resp in zip(raw_todos, resp_todos)
        ],
    }

    db_query = {"id": {"$eq": todolist_id}}
    db_results = await sql_store.find(SqlTodoList, query=db_query, limit=1)
    record_in_db = db_results[0].model_dump()

    assert got == expected
    assert record_in_db == expected


# @pytest.mark.asyncio
# @pytest.mark.parametrize("todolist", TODO_LISTS)
# async def test_create_redis_todolist(
#     client_with_redis: TestClient,
#     redis_store: RedisStore,
#     todolist: dict,
# ):
#     """POST to /todos creates a todolist in redis and returns it"""
#     with client_with_redis as client:
#         response = client.post("/todos", json=todolist)
#
#         got = response.json()
#         todolist_id = got["id"]
#         raw_todos = todolist.get("todos", [])
#         resp_todos = got["todos"]
#         expected = {
#             "id": todolist_id,
#             "name": todolist["name"],
#             "pk": todolist_id,
#             "todos": [
#                 {
#                     **raw,
#                     "is_complete": "0",
#                     "id": resp["id"],
#                     "pk": resp["pk"],
#                 }
#                 for raw, resp in zip(raw_todos, resp_todos)
#             ],
#         }
#
#         db_query = {"id": {"$eq": todolist_id}}
#         db_results = await redis_store.find(RedisTodoList, query=db_query, limit=1)
#         record_in_db = db_results[0].model_dump()
#
#         assert got == expected
#         assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("todolist", TODO_LISTS)
async def test_create_mongo_todolist(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    todolist: dict,
):
    """POST to /todos creates a todolist in redis and returns it"""
    with client_with_mongo as client:
        response = client.post("/todos", json=todolist)

        got = response.json()
        todolist_id = got["id"]
        raw_todos = todolist.get("todos", [])
        expected = {
            "id": todolist_id,
            "name": todolist["name"],
            "todos": [
                {
                    **raw,
                    "is_complete": "0",
                }
                for raw in raw_todos
            ],
        }

        db_query = {"_id": {"$eq": ObjectId(todolist_id)}}
        db_results = await mongo_store.find(MongoTodoList, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(TODO_LISTS)))
async def test_update_sql_todolist(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    sql_todolists: list[SqlTodoList],
    index: int,
):
    """PUT to /todos/{id} updates the sql todolist of given id and returns updated version"""
    with client_with_sql as client:
        todolist = sql_todolists[index]
        id_ = todolist.id
        todos = [{**v.model_dump(), "is_complete": "1"} for v in todolist.todos]
        update = {
            "name": "some other name",
            "todos": [*todos, {"title": "another one"}, {"title": "another one again"}],
        }

        response = client.put(f"/todos/{id_}", json=update)

        got = response.json()
        expected = {
            **todolist.model_dump(mode="json"),
            **update,
            "todos": [
                {
                    **raw,
                    "id": final["id"],
                    "parent_id": final["parent_id"],
                    "is_complete": raw.get("is_complete", final["is_complete"]),
                }
                for raw, final in zip(update["todos"], got["todos"])
            ],
        }
        db_query = {"id": {"$eq": id_}}
        db_results = await sql_store.find(SqlTodoList, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


# @pytest.mark.asyncio
# @pytest.mark.parametrize("client, store, model, todolists, index", _PUT_DEL_GET_PARAMS)
# async def test_update_redis_todolist(
#     client: TestClient,
#     index: int,
#     store: BaseStore,
#     model: type[BaseModel],
#     todolists: list[BaseModel],
# ):
#     """PUT to /todos/{id} updates the todolist of given id and returns updated version"""
#     todolist = todolists[index]
#     id_ = todolist.id
#     todos = [{**v.model_dump(), "is_complete": "1"} for v in todolist.todos]
#     update = {
#         "name": "some other name",
#         "todos": [*todos, {"title": "another one"}, {"title": "another one again"}],
#     }
#
#     response = client.put(f"/todos/{id_}", json=update)
#
#     got = response.json()
#     expected = todolist.model_copy(update=update).model_dump()
#     db_query = {"id": {"$eq": id_}}
#     db_results = await store.find(model, query=db_query, limit=1)
#     record_in_db = db_results[0].model_dump()
#
#     assert got == expected
#     assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(TODO_LISTS)))
async def test_update_mongo_todolist(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    mongo_todolists: list[MongoTodoList],
    index: int,
):
    """PUT to /todos/{id} updates the mongo todolist of given id and returns updated version"""
    with client_with_mongo as client:
        todolist = mongo_todolists[index]
        id_ = str(todolist.id)
        todos = [{**v.model_dump(), "is_complete": "1"} for v in todolist.todos]
        update = {
            "name": "some other name",
            "todos": [*todos, {"title": "another one"}, {"title": "another one again"}],
        }

        response = client.put(f"/todos/{id_}", json=update)

        got = response.json()
        expected = {
            **todolist.model_dump(mode="json"),
            **update,
            "todos": [
                {**v, "is_complete": v.get("is_complete", "0")} for v in update["todos"]
            ],
        }
        db_query = {"_id": {"$eq": ObjectId(id_)}}
        db_results = await mongo_store.find(MongoTodoList, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

    assert got == expected
    assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(TODO_LISTS)))
async def test_delete_sql_todolist(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    sql_todolists: list[SqlTodoList],
    index: int,
):
    """DELETE /todos/{id} deletes the sql todolist of given id and returns deleted version"""
    with client_with_sql as client:
        todolist = sql_todolists[index]
        id_ = todolist.id

        response = client.delete(f"/todos/{id_}")

        got = response.json()
        expected = todolist.model_dump(mode="json")

        db_query = {"id": {"$eq": id_}}
        db_results = await sql_store.find(SqlTodoList, query=db_query, limit=1)

        assert got == expected
        assert db_results == []


# @pytest.mark.asyncio
# @pytest.mark.parametrize("client, store, model, todolists, index", _PUT_DEL_GET_PARAMS)
# async def test_delete_redis_todolist(
#     client: TestClient,
#     index: int,
#     store: BaseStore,
#     model: type[BaseModel],
#     todolists: list[BaseModel],
# ):
#     """DELETE /todos/{id} deletes the redis todolist of given id and returns deleted version"""
#     todolist = todolists[index]
#     id_ = todolist.id
#
#     response = client.delete(f"/todos/{id_}")
#
#     got = response.json()
#     expected = todolist.model_dump()
#
#     db_query = {"id": {"$eq": id_}}
#     db_results = await store.find(model, query=db_query, limit=1)
#
#     assert got == expected
#     assert db_results == []


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(TODO_LISTS)))
async def test_delete_mongo_todolist(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    mongo_todolists: list[MongoTodoList],
    index: int,
):
    """DELETE /todos/{id} deletes the mongo todolist of given id and returns deleted version"""
    with client_with_mongo as client:
        todolist = mongo_todolists[index]
        id_ = str(todolist.id)

        response = client.delete(f"/todos/{id_}")

        got = response.json()
        expected = todolist.model_dump(mode="json")

        db_query = {"_id": {"$eq": ObjectId(id_)}}
        db_results = await mongo_store.find(MongoTodoList, query=db_query, limit=1)

        assert got == expected
        assert db_results == []


@pytest.mark.asyncio
@pytest.mark.parametrize("client, store, model, todolists, index", _PUT_DEL_GET_PARAMS)
async def test_read_one_todolist(
    client: TestClient,
    index: int,
    store: BaseStore,
    model: type[BaseModel],
    todolists: list[BaseModel],
):
    """GET /todos/{id} gets the todolist of given id"""
    todolist = todolists[index]
    id_ = todolist.id

    response = client.get(f"/todos/{id_}")

    got = response.json()
    expected = todolist.model_dump()

    db_query = {"id": {"$eq": id_}}
    db_results = await store.find(model, query=db_query, limit=1)
    record_in_db = db_results[0].model_dump()

    assert got == expected
    assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("client, store, model, todolists, q", _SEARCH_PARAMS)
async def test_search_by_name(
    client: TestClient,
    store: BaseStore,
    model: type[BaseModel],
    todolists: list[BaseModel],
    q: str,
):
    """GET /todos?q={} gets all todos with name containing search item"""
    response = client.get(f"/todos?q={q}")

    got = response.json()
    expected = [v.model_dump() for v in todolists if q in v.name.lower()]

    assert got == expected


def _get_id(item: Any) -> Any:
    """Gets the id of the given record

    Args:
        item: the record whose id is to be obtained

    Returns:
        the id of the record
    """
    try:
        return item.id
    except AttributeError:
        return item.pk
