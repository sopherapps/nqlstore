"""Deal with authentication for the app"""

import os
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError
from models import MongoInternalAuthor, RedisInternalAuthor, SqlInternalAuthor
from passlib.context import CryptContext
from stores import MongoStoreDep, RedisStoreDep, SqlStoreDep

_ALGORITHM = "HS256"
_InternalAuthorModel = MongoInternalAuthor | SqlInternalAuthor | RedisInternalAuthor

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="signin")


def verify_password(plain_password, hashed_password):
    """Verify that the passwords match

    Args:
        plain_password: the plain password
        hashed_password: the hashed password

    Returns:
        True if the passwords match else False
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Creates a hash from the given password

    Args:
        password: the password to hash

    Returns:
        the password hash
    """
    return pwd_context.hash(password)


def get_jwt_secret() -> str:
    """Gets the JWT secret from the environment"""
    return os.environ.get("JWT_SECRET")


async def get_user(
    sql_store: SqlStoreDep,
    redis_store: RedisStoreDep,
    mongo_store: MongoStoreDep,
    query: dict[str, Any],
) -> _InternalAuthorModel:
    """Gets the user instance that matches the given query

    Args:
        sql_store: the SQL store from which to retrieve the user
        redis_store: the redis store from which to retrieve the user
        mongo_store: the mongo store from which to retrieve the user
        query: the filter that the user must match

    Returns:
        the matching user

    Raises:
        HTTPException: Unauthorized
    """
    try:
        results = []
        if sql_store:
            results += await sql_store.find(SqlInternalAuthor, query=query, limit=1)

        if mongo_store:
            results += await mongo_store.find(MongoInternalAuthor, query=query, limit=1)

        if redis_store:
            results += await redis_store.find(RedisInternalAuthor, query=query, limit=1)

        return results[0]
    except (InvalidTokenError, IndexError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
        )


async def authenticate_user(
    sql_store: SqlStoreDep,
    redis_store: RedisStoreDep,
    mongo_store: MongoStoreDep,
    email: str,
    password: str,
) -> _InternalAuthorModel:
    """Authenticates the user of the given email and password

    Args:
        sql_store: the SQL store from which to retrieve the user
        redis_store: the redis store from which to retrieve the user
        mongo_store: the mongo store from which to retrieve the user
        email: the email of the user
        password: the password of the user

    Returns:
        the authenticated user or False
    """
    user = await get_user(
        sql_store=sql_store,
        redis_store=redis_store,
        mongo_store=mongo_store,
        query={"email": {"$eq": email}},
    )
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(secret_key: str, data: dict, ttl_minutes: float = 15) -> str:
    """Creates an access token given a secret key

    Args:
        secret_key: the JWT secret key for creating the JWT
        data: the data to encode
        ttl_minutes: the time to live in minutes

    Returns:
        the access token
    """
    expire = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    encoded_jwt = jwt.encode({**data, "exp": expire}, secret_key, algorithm=_ALGORITHM)
    return encoded_jwt


async def get_current_user(
    sql_store: SqlStoreDep,
    redis_store: RedisStoreDep,
    mongo_store: MongoStoreDep,
    secret_key: Annotated[str, Depends(get_jwt_secret)],
    token: Annotated[str, Depends(oauth2_scheme)],
) -> _InternalAuthorModel:
    """Gets the current user for the given token

    Args:
        sql_store: the SQL store from which to retrieve the user
        redis_store: the redis store from which to retrieve the user
        mongo_store: the mongo store from which to retrieve the user
        secret_key: the secret key for JWT decoding
        token: the token for the current user

    Returns:
        the user

    Raises:
        HTTPException: Could not validate credentials
    """
    payload = jwt.decode(token, secret_key, algorithms=[_ALGORITHM])
    email = payload.get("sub")
    return await get_user(
        sql_store=sql_store,
        redis_store=redis_store,
        mongo_store=mongo_store,
        query={"email": {"$eq": email}},
    )


# Dependencies
SecretKeyDep = Annotated[str, Depends(get_jwt_secret)]
CurrentUserDep = Annotated[_InternalAuthorModel, Depends(get_current_user)]
