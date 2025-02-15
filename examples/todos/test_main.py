from typing import Any

import pytest
from conftest import TODO_LISTS, lazy_fixture
from fastapi.testclient import TestClient
from main import MongoTodoList, RedisTodoList, SqlTodoList

from nqlstore._base import BaseModel, BaseStore

STORE_MODEL_PAIRS = [
    (lazy_fixture("sql_store"), SqlTodoList),
    (lazy_fixture("redis_store"), RedisTodoList),
    (lazy_fixture("mongo_store"), MongoTodoList),
]

STORE_MODEL_TODO_LISTS_TUPLES = [
    (lazy_fixture("sql_store"), SqlTodoList, lazy_fixture("sql_todolists")),
    (lazy_fixture("redis_store"), RedisTodoList, lazy_fixture("redis_todolists")),
    (lazy_fixture("mongo_store"), MongoTodoList, lazy_fixture("mongo_todolists")),
]

_POST_PARAMS = [
    (store, model, todolist)
    for store, model in STORE_MODEL_PAIRS
    for todolist in TODO_LISTS
]
_PUT_DEL_GET_PARAMS = [
    (store, model, todolists, index)
    for store, model, todolists in STORE_MODEL_TODO_LISTS_TUPLES
    for index in range(len(TODO_LISTS))
]
_SEARCH_PARAMS = [
    (store, model, todolist, q)
    for store, model in STORE_MODEL_PAIRS
    for todolist in TODO_LISTS
    for q in ["ho", "oo", "work"]
]


@pytest.mark.asyncio
@pytest.mark.parametrize("store, model, todolist", _POST_PARAMS)
async def test_create_todolist(
    client: TestClient,
    store: BaseStore,
    model: type[BaseModel],
    todolist: dict,
):
    """POST to /todos creates an empty todolist and returns it"""
    # using context manager to ensure on_startup runs
    with client as client:
        response = client.post("/todos", json=todolist)

        got = response.json()
        expected = {
            "id": got["id"],
            "name": todolist["name"],
            "todos": [
                {**original, **_subdict(final, ["id"])}
                for original, final in zip(
                    todolist.get("todos", []), got.get("todos", [])
                )
            ],
        }
        db_query = {"id": {"$eq": got["id"]}}
        db_results = await store.find(model, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump()

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("store, model, todolists, index", _PUT_DEL_GET_PARAMS)
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

    # using context manager to ensure on_startup runs
    with client as client:
        response = client.put(f"/todos/{id_}", json=update)

        got = response.json()
        expected = todolist.model_copy(update=update).model_dump()
        db_query = {"id": {"$eq": id_}}
        db_results = await store.find(model, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump()

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("store, model, todolists, index", _PUT_DEL_GET_PARAMS)
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

    # using context manager to ensure on_startup runs
    with client as client:
        response = client.delete(f"/todos/{id_}")

        got = response.json()
        expected = todolist.model_dump()

        db_query = {"id": {"$eq": id_}}
        db_results = await store.find(model, query=db_query, limit=1)

        assert got == expected
        assert db_results == []


@pytest.mark.asyncio
@pytest.mark.parametrize("store, model, todolists, index", _PUT_DEL_GET_PARAMS)
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

    # using context manager to ensure on_startup runs
    with client as client:
        response = client.get(f"/todos/{id_}")

        got = response.json()
        expected = todolist.model_dump()

        db_query = {"id": {"$eq": id_}}
        db_results = await store.find(model, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump()

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("store, model, todolists, q", _SEARCH_PARAMS)
async def test_search_by_name(
    client: TestClient,
    store: BaseStore,
    model: type[BaseModel],
    todolists: list[BaseModel],
    q: str,
):
    """GET /todos?q={} gets all todos with name containing search item"""
    # using context manager to ensure on_startup runs
    with client as client:
        response = client.get(f"/todos?q={q}")

        got = response.json()
        expected = [v.model_dump() for v in todolists if q in v.name.lower()]

        assert got == expected


def _subdict(data: dict, include: list[str]) -> dict:
    """Copies a subset of the dict with only the given keys if they exist

    Args:
        include: the keys to include

    Returns:
        dictionary with only the given keys if they exist
    """
    return {k: v for k, v in data.items() if k in include}


def _get_id(item: Any) -> Any:
    """Gets the id of the given record

    Args:
        item: the reocrd whose id is to be obtained

    Returns:
        the id of the record
    """
    try:
        return item.id
    except AttributeError:
        return item.pk
