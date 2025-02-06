"""Module to cater for missing dependencies"""

from typing import Any, Callable, Literal

"""
redis imports; and their defaults if redis_om is missing
"""
try:
    from aredis_om import EmbeddedJsonModel as _EmbeddedJsonModel
    from aredis_om import HashModel as _HashModel
    from aredis_om import JsonModel as _JsonModel
    from aredis_om import KNNExpression, Migrator
    from aredis_om import RedisModel as _RedisModel
    from aredis_om import get_redis_connection
    from aredis_om.model.model import Expression
    from aredis_om.model.model import Field as _RedisField
    from aredis_om.model.model import FieldInfo as _RedisFieldInfo
    from aredis_om.model.model import VectorFieldOptions, verify_pipeline_response
    from redis.client import Pipeline
except ImportError:
    from pydantic.fields import Field as _RedisField
    from pydantic.fields import FieldInfo as _RedisFieldInfo
    from pydantic.main import BaseModel

    VectorFieldOptions = Any
    _EmbeddedJsonModel = BaseModel
    _HashModel = BaseModel
    _JsonModel = BaseModel
    _RedisModel = BaseModel
    KNNExpression = Any
    Expression = Any
    Pipeline = Any
    Migrator = lambda *a, **k: dict(**k)
    get_redis_connection = lambda *a, **k: dict(**k)
    verify_pipeline_response = lambda *a, **k: dict(**k)


"""
sql imports; and their default if sqlmodel is missing
"""
try:
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.sql._typing import (
        _ColumnExpressionArgument,
        _ColumnExpressionOrStrLabelArgument,
    )
    from sqlmodel import SQLModel as _SQLModel
    from sqlmodel import delete, insert, select, update
    from sqlmodel._compat import post_init_field_info
    from sqlmodel.ext.asyncio.session import AsyncSession
    from sqlmodel.main import Column
    from sqlmodel.main import Field as _SQLField
    from sqlmodel.main import FieldInfo as _SqlFieldInfo
    from sqlmodel.main import NoArgAnyCallable, OnDeleteType, Relationship
except ImportError:
    from typing import Set as _ColumnExpressionArgument
    from typing import Set as _ColumnExpressionOrStrLabelArgument

    from pydantic import BaseModel as _SQLModel
    from pydantic.fields import Field as Relationship
    from pydantic.fields import Field as _SQLField
    from pydantic.fields import FieldInfo as _FieldInfo

    class _SqlFieldInfo(_FieldInfo): ...

    post_init_field_info = lambda b: b
    NoArgAnyCallable = Callable[[], Any]
    OnDeleteType = Literal["CASCADE", "SET NULL", "RESTRICT"]
    Column = Any
    create_async_engine = lambda *a, **k: dict(**k)
    delete = insert = select = update = create_async_engine
    AsyncSession = Any


"""
mongo imports; and their defaults if the 'beanie' package is not installed
"""
try:
    from beanie import (
        BulkWriter,
        Document,
        PydanticObjectId,
        SortDirection,
        WriteRules,
        init_beanie,
    )
    from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
except ImportError:
    from pydantic import BaseModel as Document

    init_beanie = lambda *a, **k: dict(**k)
    BulkWriter = Any
    SortDirection = Any
    AsyncIOMotorClient = lambda *a, **k: dict(**k)
    AsyncIOMotorClientSession = Any
    PydanticObjectId = Any

    import enum

    class WriteRules(str, enum.Enum):
        DO_NOTHING = "DO_NOTHING"
        WRITE = "WRITE"
