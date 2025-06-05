import logging
from contextlib import asynccontextmanager
from typing import Annotated

from auth import (
    CurrentUserDep,
    SecretKeyDep,
    authenticate_user,
    create_access_token,
    get_password_hash,
)
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import OAuth2PasswordRequestForm
from models import MongoPost, RedisPost, SqlInternalAuthor, SqlPost
from pydantic import BaseModel
from schemas import InternalAuthor, PartialPost, Post, TokenResponse
from stores import MongoStoreDep, RedisStoreDep, SqlStoreDep, clear_stores

_ACCESS_TOKEN_EXPIRE_MINUTES = 30


@asynccontextmanager
async def lifespan(app_: FastAPI):
    clear_stores()
    yield
    clear_stores()


app = FastAPI(lifespan=lifespan)


@app.post("/signup")
async def signup(
    sql: SqlStoreDep,
    redis: RedisStoreDep,
    mongo: MongoStoreDep,
    payload: InternalAuthor,
):
    """Signup a new user"""
    results = []
    payload_dict = payload.model_dump(exclude_unset=True)
    payload_dict["password"] = get_password_hash(payload_dict["password"])

    try:
        if sql:
            results += await sql.insert(SqlInternalAuthor, [payload_dict])

        if redis:
            results += await redis.insert(RedisPost, [payload_dict])

        if mongo:
            results += await mongo.insert(MongoPost, [payload_dict])

        result = results[0].model_dump(mode="json")
        return result
    except Exception as exp:
        logging.error(exp)
        raise exp


@app.post("/signin")
async def signin(
    sql: SqlStoreDep,
    redis: RedisStoreDep,
    mongo: MongoStoreDep,
    secret_key: SecretKeyDep,
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> TokenResponse:
    user = await authenticate_user(
        sql_store=sql,
        redis_store=redis,
        mongo_store=mongo,
        email=form_data.username,
        password=form_data.password,
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(
        secret_key=secret_key,
        data={"sub": user.username},
        ttl_minutes=_ACCESS_TOKEN_EXPIRE_MINUTES,
    )
    return TokenResponse(access_token=access_token, token_type="bearer")


@app.get("/posts")
async def search(
    sql: SqlStoreDep,
    redis: RedisStoreDep,
    mongo: MongoStoreDep,
    title: str | None = Query(None),
    author: str | None = Query(None),
    tag: str | None = Query(None),
):
    """Searches for posts by title, author or tag"""

    results = []
    query_dict: dict[str, str] = {
        k: v
        for k, v in {"title": title, "author.name": author, "tags.title": tag}.items()
        if v
    }
    query = {k: {"$regex": f".*{v}.*", "$options": "i"} for k, v in query_dict.items()}
    try:
        if sql:
            results += await sql.find(SqlPost, query=query)

        if redis:
            # redis's regex search is not mature so we use its full text search
            # Unfortunately, redis search does not permit us to search fields that are arrays.
            redis_query = [
                (
                    (_get_redis_field(RedisPost, k) == f"{v}")
                    if k == "tags.title"
                    else (_get_redis_field(RedisPost, k) % f"*{v}*")
                )
                for k, v in query_dict.items()
            ]
            results += await redis.find(RedisPost, *redis_query)

        if mongo:
            results += await mongo.find(MongoPost, query=query)
    except Exception as exp:
        logging.error(exp)
        raise exp

    return [item.model_dump(mode="json") for item in results]


@app.get("/posts/{id_}")
async def get_one(
    sql: SqlStoreDep,
    redis: RedisStoreDep,
    mongo: MongoStoreDep,
    id_: int | str,
):
    """Get post by id"""
    results = []
    query = {"id": {"$eq": id_}}

    try:
        if sql:
            results += await sql.find(SqlPost, query=query, limit=1)

        if redis:
            results += await redis.find(RedisPost, query=query, limit=1)

        if mongo:
            results += await mongo.find(MongoPost, query=query, limit=1)
    except Exception as exp:
        logging.error(exp)
        raise exp

    try:
        return results[0].model_dump(mode="json")
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


@app.post("/posts")
async def create_one(
    sql: SqlStoreDep,
    redis: RedisStoreDep,
    mongo: MongoStoreDep,
    current_user: CurrentUserDep,
    payload: Post,
):
    """Create a post"""
    results = []
    payload_dict = payload.model_dump(exclude_unset=True)
    payload_dict["author"] = current_user.model_dump()

    try:
        if sql:
            results += await sql.insert(SqlPost, [payload_dict])

        if redis:
            results += await redis.insert(RedisPost, [payload_dict])

        if mongo:
            results += await mongo.insert(MongoPost, [payload_dict])

        result = results[0].model_dump(mode="json")
        return result
    except Exception as exp:
        logging.error(exp)
        raise exp


@app.put("/posts/{id_}")
async def update_one(
    sql: SqlStoreDep,
    redis: RedisStoreDep,
    mongo: MongoStoreDep,
    current_user: CurrentUserDep,
    id_: int | str,
    payload: PartialPost,
):
    """Update a post"""
    results = []
    query = {"id": {"$eq": id_}}
    updates = payload.model_dump(exclude_unset=True)
    user_dict = current_user.model_dump()

    if "comments" in updates:
        # just resetting the author of all comments to current user.
        # This is probably logically wrong.
        # It is just here for illustration
        updates["comments"] = [
            {**item, "author": user_dict} for item in updates["comments"]
        ]

    try:
        if sql:
            results += await sql.update(SqlPost, query=query, updates=updates)

        if redis:
            results += await redis.update(RedisPost, query=query, updates=updates)

        if mongo:
            results += await mongo.update(MongoPost, query=query, updates=updates)
    except Exception as exp:
        logging.error(exp)
        raise exp

    try:
        return results[0].model_dump(mode="json")
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


@app.delete("/posts/{id_}")
async def delete_one(
    sql: SqlStoreDep,
    redis: RedisStoreDep,
    mongo: MongoStoreDep,
    id_: int | str,
):
    """Delete a post"""
    results = []
    query = {"id": {"$eq": id_}}

    try:
        if sql:
            results += await sql.delete(SqlPost, query=query)

        if redis:
            results += await redis.delete(RedisPost, query=query)

        if mongo:
            results += await mongo.delete(MongoPost, query=query)
    except Exception as exp:
        logging.error(exp)
        raise exp

    try:
        return results[0].model_dump(mode="json")
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


def _get_redis_field(model: type[BaseModel], path: str):
    """Retrieves the Field at the given path, which may or may not be dotted

    Args:
        path: the path to the field where dots signify relations; example books.title
        model: the parent model

    Returns:
        the FieldInfo at the given path

    Raises:
        ValueError: no field '{path}' found on '{parent}'
    """
    path_segments = path.split(".")
    current_parent = model

    field = None
    for idx, part in enumerate(path_segments):
        field = getattr(current_parent, part)
        try:
            current_parent = field
        except AttributeError as exp:
            if idx == len(path_segments) - 1:
                break
            raise exp

    if field is None:
        raise ValueError(f"no field '{path}' found on '{model}'")

    return field
