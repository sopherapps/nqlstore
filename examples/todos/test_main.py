from typing import Any

import pytest
from conftest import TODO_LISTS, lazy_fixture
from fastapi.testclient import TestClient
from main import MongoTodoList, RedisTodoList, SqlTodoList

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

_POST_PARAMS = [
    (client, store, model, todolist)
    for client, store, model in STORE_MODEL_TUPLES
    for todolist in TODO_LISTS
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
@pytest.mark.parametrize("client, store, model, todolist", _POST_PARAMS)
async def test_create_todolist(
    client: TestClient,
    store: BaseStore,
    model: type[BaseModel],
    todolist: dict,
):
    """POST to /todos creates an empty todolist and returns it"""
    response = client.post("/todos", json=todolist)

    got = response.json()
    expected = {"id": got["id"], "name": todolist["name"], "todos": []}
    for i, todo in enumerate(todolist.get("todos", [])):
        new_todo = {
            **todo,
            "is_complete": "0",
            "parent_id": got["id"],
        }
        if "id" in got["todos"][i]:
            new_todo["id"] = got["todos"][i]["id"]

        if "parent_id" in got["todos"][i]:
            new_todo["parent_id"] = got["id"]

        expected["todos"].append(new_todo)

    db_query = {"id": {"$eq": got["id"]}}
    db_results = await store.find(model, query=db_query, limit=1)
    record_in_db = db_results[0].model_dump()

    assert got == expected
    assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("client, store, model, todolists, index", _PUT_DEL_GET_PARAMS)
async def test_update_todolist(
    client: TestClient,
    index: int,
    store: BaseStore,
    model: type[BaseModel],
    todolists: list[BaseModel],
):
    """PUT to /todos/{id} updates the todolist of given id and returns updated version"""
    todolist = todolists[index]
    id_ = todolist.id
    todos = [{**v.model_dump(), "is_complete": "1"} for v in todolist.todos]
    update = {
        "name": "some other name",
        "todos": [*todos, {"title": "another one"}, {"title": "another one again"}],
    }

    response = client.put(f"/todos/{id_}", json=update)

    got = response.json()
    expected = todolist.model_copy(update=update).model_dump()
    db_query = {"id": {"$eq": id_}}
    db_results = await store.find(model, query=db_query, limit=1)
    record_in_db = db_results[0].model_dump()

    assert got == expected
    assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("client, store, model, todolists, index", _PUT_DEL_GET_PARAMS)
async def test_delete_todolist(
    client: TestClient,
    index: int,
    store: BaseStore,
    model: type[BaseModel],
    todolists: list[BaseModel],
):
    """DELETE /todos/{id} deletes the todolist of given id and returns deleted version"""
    todolist = todolists[index]
    id_ = todolist.id

    response = client.delete(f"/todos/{id_}")

    got = response.json()
    expected = todolist.model_dump()

    db_query = {"id": {"$eq": id_}}
    db_results = await store.find(model, query=db_query, limit=1)

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
