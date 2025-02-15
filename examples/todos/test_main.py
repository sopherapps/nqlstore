from typing import Any

import pytest
from conftest import STORE_MODEL_PAIRS, STORE_MODEL_TODO_LISTS_TUPLES, TODO_LISTS
from fastapi.testclient import TestClient

from nqlstore._base import BaseModel, BaseStore

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
async def test_create_sql_todolist(
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
        db_record = await store.find(model, query={"id": got["id"]}, limit=1)[
            0
        ].model_dump()

        assert got == expected
        assert db_record == expected


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
        record_in_db = await store.find(model, query={"id": id_}, limit=1)[
            0
        ].model_dump()

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
        record_in_db = await store.find(model, query={"id": id_}, limit=1)

        assert got == expected
        assert record_in_db == []


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
        record_in_db = await store.find(model, query={"id": id_}, limit=1)[
            0
        ].model_dump()

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
    """GET /todos/?q={} gets all todos with name containing search item"""
    # using context manager to ensure on_startup runs
    with client as client:
        response = client.get(f"/todos/?q={q}")

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
    return {k: v for k, v in data.items if k in include}


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
