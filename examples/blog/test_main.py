from datetime import datetime
from typing import Any

import pytest
from bson import ObjectId
from conftest import ACCESS_TOKEN, AUTHOR, COMMENT_LIST, POST_LISTS
from fastapi.testclient import TestClient
from main import MongoPost, RedisPost, SqlPost

from nqlstore import MongoStore, RedisStore, SQLStore

_TITLE_SEARCH_TERMS = ["ho", "oo", "work"]
_TAG_SEARCH_TERMS = ["art", "om"]
_HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}"}


@pytest.mark.asyncio
@pytest.mark.parametrize("post", POST_LISTS)
async def test_create_sql_post(
    client_with_sql: TestClient, sql_store: SQLStore, post: dict, freezer
):
    """POST to /posts creates a post in sql and returns it"""
    timestamp = datetime.now().isoformat()
    with client_with_sql as client:
        response = client.post("/posts", json=post, headers=_HEADERS)

        got = response.json()
        post_id = got["id"]
        raw_tags = post.get("tags", [])
        resp_tags = got["tags"]
        expected = {
            "id": post_id,
            "title": post["title"],
            "content": post.get("content", ""),
            "author": {"id": 1, **AUTHOR},
            "author_id": 1,
            "tags": [
                {
                    **raw,
                    "id": resp["id"],
                }
                for raw, resp in zip(raw_tags, resp_tags)
            ],
            "comments": [],
            "created_at": timestamp,
            "updated_at": timestamp,
        }

        db_query = {"id": {"$eq": post_id}}
        db_results = await sql_store.find(SqlPost, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump()

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("post", POST_LISTS)
async def test_create_redis_post(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    post: dict,
    freezer,
):
    """POST to /posts creates a post in redis and returns it"""
    timestamp = datetime.now().isoformat()
    with client_with_redis as client:
        response = client.post("/posts", json=post, headers=_HEADERS)

        got = response.json()
        post_id = got["id"]
        raw_tags = post.get("tags", [])
        resp_tags = got["tags"]
        expected = {
            "id": post_id,
            "title": post["title"],
            "content": post.get("content", ""),
            "author": {**got["author"], **AUTHOR},
            "pk": post_id,
            "tags": [
                {
                    **raw,
                    "id": resp["id"],
                    "pk": resp["pk"],
                }
                for raw, resp in zip(raw_tags, resp_tags)
            ],
            "comments": [],
            "created_at": timestamp,
            "updated_at": timestamp,
        }

        db_query = {"id": {"$eq": post_id}}
        db_results = await redis_store.find(RedisPost, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("post", POST_LISTS)
async def test_create_mongo_post(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    post: dict,
    freezer,
):
    """POST to /posts creates a post in redis and returns it"""
    timestamp = datetime.now().isoformat()
    with client_with_mongo as client:
        response = client.post("/posts", json=post, headers=_HEADERS)

        got = response.json()
        post_id = got["id"]
        raw_tags = post.get("tags", [])
        expected = {
            "id": post_id,
            "title": post["title"],
            "content": post.get("content", ""),
            "author": {"name": AUTHOR["name"]},
            "tags": raw_tags,
            "comments": [],
            "created_at": timestamp,
            "updated_at": timestamp,
        }

        db_query = {"_id": {"$eq": ObjectId(post_id)}}
        db_results = await mongo_store.find(MongoPost, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(POST_LISTS)))
async def test_update_sql_post(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    sql_posts: list[SqlPost],
    index: int,
    freezer,
):
    """PUT to /posts/{id} updates the sql post of given id and returns updated version"""
    timestamp = datetime.now().isoformat()
    with client_with_sql as client:
        post = sql_posts[index]
        post_dict = post.model_dump(mode="json", exclude_none=True, exclude_unset=True)
        id_ = post.id
        update = {
            **post_dict,
            "title": "some other title",
            "tags": [
                *post_dict["tags"],
                {"title": "another one"},
                {"title": "another one again"},
            ],
            "comments": [*post_dict["comments"], *COMMENT_LIST[index:]],
        }

        response = client.put(f"/posts/{id_}", json=update, headers=_HEADERS)

        got = response.json()
        expected = {
            **post.model_dump(mode="json"),
            **update,
            "comments": [
                {
                    **raw,
                    "id": final["id"],
                    "post_id": final["post_id"],
                    "author_id": 1,
                    "created_at": timestamp,
                    "updated_at": timestamp,
                }
                for raw, final in zip(update["comments"], got["comments"])
            ],
            "tags": [
                {
                    **raw,
                    "id": final["id"],
                }
                for raw, final in zip(update["tags"], got["tags"])
            ],
        }
        db_query = {"id": {"$eq": id_}}
        db_results = await sql_store.find(SqlPost, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(POST_LISTS)))
async def test_update_redis_post(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    redis_posts: list[RedisPost],
    index: int,
    freezer,
):
    """PUT to /posts/{id} updates the redis post of given id and returns updated version"""
    timestamp = datetime.now().isoformat()
    with client_with_redis as client:
        post = redis_posts[index]
        post_dict = post.model_dump(mode="json", exclude_none=True, exclude_unset=True)
        id_ = post.id
        update = {
            "title": "some other title",
            "tags": [
                *post_dict.get("tags", []),
                {"title": "another one"},
                {"title": "another one again"},
            ],
            "comments": [*post_dict.get("comments", []), *COMMENT_LIST[index:]],
        }

        response = client.put(f"/posts/{id_}", json=update, headers=_HEADERS)

        got = response.json()
        expected = {
            **post.model_dump(mode="json"),
            **update,
            "comments": [
                {
                    **raw,
                    "id": final["id"],
                    "author": final["author"],
                    "pk": final["pk"],
                    "created_at": timestamp,
                    "updated_at": timestamp,
                }
                for raw, final in zip(update["comments"], got["comments"])
            ],
            "tags": [
                {
                    **raw,
                    "id": final["id"],
                    "pk": final["pk"],
                }
                for raw, final in zip(update["tags"], got["tags"])
            ],
        }
        db_query = {"id": {"$eq": id_}}
        db_results = await redis_store.find(RedisPost, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")
        expected_in_db = {
            **expected,
            "tags": [
                {
                    **raw,
                    "id": final["id"],
                    "pk": final["pk"],
                }
                for raw, final in zip(expected["tags"], record_in_db["tags"])
            ],
            "comments": [
                {
                    **raw,
                    "id": final["id"],
                    "pk": final["pk"],
                }
                for raw, final in zip(expected["comments"], record_in_db["comments"])
            ],
        }

    assert got == expected
    assert record_in_db == expected_in_db


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(POST_LISTS)))
async def test_update_mongo_post(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    mongo_posts: list[MongoPost],
    index: int,
    freezer,
):
    """PUT to /posts/{id} updates the mongo post of given id and returns updated version"""
    timestamp = datetime.now().isoformat()
    with client_with_mongo as client:
        post = mongo_posts[index]
        post_dict = post.model_dump(mode="json", exclude_none=True, exclude_unset=True)
        id_ = post.id
        update = {
            "title": "some other title",
            "tags": [
                *post_dict.get("tags", []),
                {"title": "another one"},
                {"title": "another one again"},
            ],
            "comments": [*post_dict.get("comments", []), *COMMENT_LIST[index:]],
        }

        response = client.put(f"/posts/{id_}", json=update, headers=_HEADERS)

        got = response.json()
        expected = {
            **post.model_dump(mode="json"),
            **update,
            "comments": [
                {
                    **raw,
                    "author": final["author"],
                    "created_at": timestamp,
                    "updated_at": timestamp,
                }
                for raw, final in zip(update["comments"], got["comments"])
            ],
        }
        db_query = {"_id": {"$eq": id_}}
        db_results = await mongo_store.find(MongoPost, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

    assert got == expected
    assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(POST_LISTS)))
async def test_delete_sql_post(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    sql_posts: list[SqlPost],
    index: int,
):
    """DELETE /posts/{id} deletes the sql post of given id and returns deleted version"""
    with client_with_sql as client:
        post = sql_posts[index]
        id_ = post.id

        response = client.delete(f"/posts/{id_}")

        got = response.json()
        expected = post.model_dump(mode="json")

        db_query = {"id": {"$eq": id_}}
        db_results = await sql_store.find(SqlPost, query=db_query, limit=1)

        assert got == expected
        assert db_results == []


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(POST_LISTS)))
async def test_delete_redis_post(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    redis_posts: list[RedisPost],
    index: int,
):
    """DELETE /posts/{id} deletes the redis post of given id and returns deleted version"""
    with client_with_redis as client:
        post = redis_posts[index]
        id_ = post.id

        response = client.delete(f"/posts/{id_}")

        got = response.json()
        expected = post.model_dump(mode="json")

        db_query = {"id": {"$eq": id_}}
        db_results = await redis_store.find(RedisPost, query=db_query, limit=1)

        assert got == expected
        assert db_results == []


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(POST_LISTS)))
async def test_delete_mongo_post(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    mongo_posts: list[MongoPost],
    index: int,
):
    """DELETE /posts/{id} deletes the mongo post of given id and returns deleted version"""
    with client_with_mongo as client:
        post = mongo_posts[index]
        id_ = post.id

        response = client.delete(f"/posts/{id_}")

        got = response.json()
        expected = post.model_dump(mode="json")

        db_query = {"_id": {"$eq": id_}}
        db_results = await mongo_store.find(MongoPost, query=db_query, limit=1)

        assert got == expected
        assert db_results == []


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(POST_LISTS)))
async def test_read_one_sql_post(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    sql_posts: list[SqlPost],
    index: int,
):
    """GET /posts/{id} gets the sql post of given id"""
    with client_with_sql as client:
        post = sql_posts[index]
        id_ = post.id

        response = client.get(f"/posts/{id_}")

        got = response.json()
        expected = post.model_dump(mode="json")

        db_query = {"id": {"$eq": id_}}
        db_results = await sql_store.find(SqlPost, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(POST_LISTS)))
async def test_read_one_redis_post(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    redis_posts: list[RedisPost],
    index: int,
):
    """GET /posts/{id} gets the redis post of given id"""
    with client_with_redis as client:
        post = redis_posts[index]
        id_ = post.id

        response = client.get(f"/posts/{id_}")

        got = response.json()
        expected = post.model_dump(mode="json")

        db_query = {"id": {"$eq": id_}}
        db_results = await redis_store.find(RedisPost, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("index", range(len(POST_LISTS)))
async def test_read_one_mongo_post(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    mongo_posts: list[MongoPost],
    index: int,
):
    """GET /posts/{id} gets the mongo post of given id"""
    with client_with_mongo as client:
        post = mongo_posts[index]
        id_ = post.id

        response = client.get(f"/posts/{id_}")

        got = response.json()
        expected = post.model_dump(mode="json")

        db_query = {"_id": {"$eq": id_}}
        db_results = await mongo_store.find(MongoPost, query=db_query, limit=1)
        record_in_db = db_results[0].model_dump(mode="json")

        assert got == expected
        assert record_in_db == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("q", _TITLE_SEARCH_TERMS)
async def test_search_sql_by_title(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    sql_posts: list[SqlPost],
    q: str,
):
    """GET /posts?title={} gets all sql posts with title containing search item"""
    with client_with_sql as client:
        response = client.get(f"/posts?title={q}")

        got = response.json()
        expected = [
            v.model_dump(mode="json") for v in sql_posts if q in v.title.lower()
        ]

        assert got == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("q", _TITLE_SEARCH_TERMS)
async def test_search_redis_by_title(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    redis_posts: list[RedisPost],
    q: str,
):
    """GET /posts?title={} gets all redis posts with title containing search item"""
    with client_with_redis as client:
        response = client.get(f"/posts?title={q}")

        got = response.json()
        expected = [
            v.model_dump(mode="json") for v in redis_posts if q in v.title.lower()
        ]

        assert got == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("q", _TITLE_SEARCH_TERMS)
async def test_search_mongo_by_title(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    mongo_posts: list[MongoPost],
    q: str,
):
    """GET /posts?title={} gets all mongo posts with title containing search item"""
    with client_with_mongo as client:
        response = client.get(f"/posts?title={q}")

        got = response.json()
        expected = [
            v.model_dump(mode="json") for v in mongo_posts if q in v.title.lower()
        ]

        assert got == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("q", _TAG_SEARCH_TERMS)
async def test_search_sql_by_tag(
    client_with_sql: TestClient,
    sql_store: SQLStore,
    sql_posts: list[SqlPost],
    q: str,
):
    """GET /posts?tag={} gets all sql posts with tag containing search item"""
    with client_with_sql as client:
        response = client.get(f"/posts?tag={q}")

        got = response.json()
        expected = [
            v.model_dump(mode="json")
            for v in sql_posts
            if any([q in tag.title.lower() for tag in v.tags])
        ]

        assert got == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("q", ["random", "another one", "another one again"])
async def test_search_redis_by_tag(
    client_with_redis: TestClient,
    redis_store: RedisStore,
    redis_posts: list[RedisPost],
    q: str,
):
    """GET /posts?tag={} gets all redis posts with tag containing search item. Partial searches nit supported."""
    with client_with_redis as client:
        response = client.get(f"/posts?tag={q}")

        got = response.json()
        expected = [
            v.model_dump(mode="json")
            for v in redis_posts
            if any([q in tag.title.lower() for tag in v.tags])
        ]

        assert got == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("q", _TAG_SEARCH_TERMS)
async def test_search_mongo_by_tag(
    client_with_mongo: TestClient,
    mongo_store: MongoStore,
    mongo_posts: list[MongoPost],
    q: str,
):
    """GET /posts?tag={} gets all mongo posts with tag containing search item"""
    with client_with_mongo as client:
        response = client.get(f"/posts?tag={q}")

        got = response.json()
        expected = [
            v.model_dump(mode="json")
            for v in mongo_posts
            if any([q in tag.title.lower() for tag in v.tags])
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
