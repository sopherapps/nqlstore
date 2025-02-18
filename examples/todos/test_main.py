from typing import Any

import pytest
from bson import ObjectId
from conftest import TODO_LISTS
from fastapi.testclient import TestClient
from main import MongoTodoList, RedisTodoList, SqlTodoList

from nqlstore import MongoStore, RedisStore, SQLStore

_SEARCH_TERMS = ["ho", "oo", "work"]


@pytest.mark.asyncio
@pytest.mark.parametrize("todolist", TODO_LISTS)
async def test_create_sql_todolist(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    todolist: dict,
):
    """POST to /todos creates a todolist in sql and returns it"""
    with client_with_sql as client:
        response = client.post("/todos", json=todolist)

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


@pytest.mark.asyncio
@pytest.mark.parametrize("todolist", TODO_LISTS)
async def test_create_redis_todolist(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    todolist: dict,
):
    """POST to /todos creates a todolist in redis and returns it"""
    with client_with_redis as client:
        response = client.post("/todos", json=todolist)

        got = response.json()
        todolist_id = got["id"]
        raw_todos = todolist.get("todos", [])
        resp_todos = got["todos"]
        expected = {
            "id": todolist_id,
            "name": todolist["name"],
            "pk": todolist_id,
            "todos": [
                {
                    **raw,
                    "is_complete": "0",
                    "id": resp["id"],
                    "pk": resp["pk"],
                }
                for raw, resp in zip(raw_todos, resp_todos)
            ],
        }

        db_query = {"id": {"$eq": todolist_id}}
        db_results = await redis_store.find(RedisTodoList, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


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


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(TODO_LISTS)))
async def test_update_redis_todolist(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    redis_todolists: list[RedisTodoList],
    index: int,
):
    """PUT to /todos/{id} updates the redis todolist of given id and returns updated version"""
    with client_with_redis as client:
        todolist = redis_todolists[index]
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
                    "pk": final["pk"],
                    "is_complete": raw.get("is_complete", final["is_complete"]),
                }
                for raw, final in zip(update["todos"], got["todos"])
            ],
        }
        db_query = {"id": {"$eq": id_}}
        db_results = await redis_store.find(RedisTodoList, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")
        expected_in_db = {
            **expected,
            "todos": [
                {
                    **raw,
                    "id": final["id"],
                    "pk": final["pk"],
                }
                for raw, final in zip(expected["todos"], record_in_db["todos"])
            ],
        }

    assert got == expected
    assert record_in_db == expected_in_db


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
                {**v, "is_complete": v.get("is_complete", "0")} for v in update["todos"]
            ],
        }
        db_query = {"_id": {"$eq": id_}}
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


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(TODO_LISTS)))
async def test_delete_redis_todolist(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    redis_todolists: list[RedisTodoList],
    index: int,
):
    """DELETE /todos/{id} deletes the redis todolist of given id and returns deleted version"""
    with client_with_redis as client:
        todolist = redis_todolists[index]
        id_ = todolist.id

        response = client.delete(f"/todos/{id_}")

        got = response.json()
        expected = todolist.model_dump(mode="json")

        db_query = {"id": {"$eq": id_}}
        db_results = await redis_store.find(RedisTodoList, query=db_query, limit=1)

        assert got == expected
        assert db_results == []


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
        id_ = todolist.id

        response = client.delete(f"/todos/{id_}")

        got = response.json()
        expected = todolist.model_dump(mode="json")

        db_query = {"_id": {"$eq": id_}}
        db_results = await mongo_store.find(MongoTodoList, query=db_query, limit=1)

        assert got == expected
        assert db_results == []


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(TODO_LISTS)))
async def test_read_one_sql_todolist(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    sql_todolists: list[SqlTodoList],
    index: int,
):
    """GET /todos/{id} gets the sql todolist of given id"""
    with client_with_sql as client:
        todolist = sql_todolists[index]
        id_ = todolist.id

        response = client.get(f"/todos/{id_}")

        got = response.json()
        expected = todolist.model_dump(mode="json")

        db_query = {"id": {"$eq": id_}}
        db_results = await sql_store.find(SqlTodoList, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(TODO_LISTS)))
async def test_read_one_redis_todolist(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    redis_todolists: list[RedisTodoList],
    index: int,
):
    """GET /todos/{id} gets the redis todolist of given id"""
    with client_with_redis as client:
        todolist = redis_todolists[index]
        id_ = todolist.id

        response = client.get(f"/todos/{id_}")

        got = response.json()
        expected = todolist.model_dump(mode="json")

        db_query = {"id": {"$eq": id_}}
        db_results = await redis_store.find(RedisTodoList, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(TODO_LISTS)))
async def test_read_one_mongo_todolist(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    mongo_todolists: list[MongoTodoList],
    index: int,
):
    """GET /todos/{id} gets the mongo todolist of given id"""
    with client_with_mongo as client:
        todolist = mongo_todolists[index]
        id_ = todolist.id

        response = client.get(f"/todos/{id_}")

        got = response.json()
        expected = todolist.model_dump(mode="json")

        db_query = {"_id": {"$eq": id_}}
        db_results = await mongo_store.find(MongoTodoList, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("q", _SEARCH_TERMS)
async def test_search_sql_by_name(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    sql_todolists: list[SqlTodoList],
    q: str,
):
    """GET /todos?q={} gets all sql todolists with name containing search item"""
    with client_with_sql as client:
        response = client.get(f"/todos?q={q}")

        got = response.json()
        expected = [
            v.model_dump(mode="json") for v in sql_todolists if q in v.name.lower()
        ]

        assert got == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("q", _SEARCH_TERMS)
async def test_search_redis_by_name(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    redis_todolists: list[RedisTodoList],
    q: str,
):
    """GET /todos?q={} gets all redis todolists with name containing search item"""
    with client_with_redis as client:
        response = client.get(f"/todos?q={q}")

        got = response.json()
        expected = [
            v.model_dump(mode="json") for v in redis_todolists if q in v.name.lower()
        ]

        assert got == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("q", _SEARCH_TERMS)
async def test_search_mongo_by_name(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    mongo_todolists: list[MongoTodoList],
    q: str,
):
    """GET /todos?q={} gets all mongo todolists with name containing search item"""
    with client_with_mongo as client:
        response = client.get(f"/todos?q={q}")

        got = response.json()
        expected = [
            v.model_dump(mode="json") for v in mongo_todolists if q in v.name.lower()
        ]

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
