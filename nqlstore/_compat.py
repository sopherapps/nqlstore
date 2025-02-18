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
    from redis.asyncio import Redis
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
    Redis = Any


"""
sql imports; and their default if sqlmodel is missing
"""
try:
    from sqlalchemy import Column, Table
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import InstrumentedAttribute, RelationshipProperty, subqueryload
    from sqlalchemy.orm.exc import DetachedInstanceError
    from sqlalchemy.sql._typing import (
        _ColumnExpressionArgument,
        _ColumnExpressionOrStrLabelArgument,
    )
    from sqlmodel import SQLModel as _SQLModel
    from sqlmodel import delete, insert, select, update
    from sqlmodel._compat import post_init_field_info
    from sqlmodel.ext.asyncio.session import AsyncSession
    from sqlmodel.main import Field as _SQLField
    from sqlmodel.main import FieldInfo as _SqlFieldInfo
    from sqlmodel.main import IncEx, NoArgAnyCallable, OnDeleteType
    from sqlmodel.main import RelationshipInfo as _RelationshipInfo
except ImportError:
    from typing import Mapping, Optional, Sequence
    from typing import Set
    from typing import Set as _ColumnExpressionArgument
    from typing import Set as _ColumnExpressionOrStrLabelArgument
    from typing import Union

    from pydantic import BaseModel
    from pydantic._internal._repr import Representation
    from pydantic.fields import Field as _SQLField
    from pydantic.fields import FieldInfo as _FieldInfo

    _SQLModel = BaseModel
    post_init_field_info = lambda b: b
    NoArgAnyCallable = Callable[[], Any]
    OnDeleteType = Literal["CASCADE", "SET NULL", "RESTRICT"]
    Column = Any
    create_async_engine = lambda *a, **k: dict(**k)
    delete = insert = select = update = create_async_engine
    AsyncSession = Any
    RelationshipProperty = Set
    Table = Set
    InstrumentedAttribute = Set
    subqueryload = lambda *a, **kwargs: dict(**kwargs)
    DetachedInstanceError = RuntimeError
    IncEx = Set[Any] | dict

    class _SqlFieldInfo(_FieldInfo): ...

    class _RelationshipInfo(Representation):
        def __init__(
            self,
            *,
            back_populates: Optional[str] = None,
            cascade_delete: Optional[bool] = False,
            passive_deletes: Optional[Union[bool, Literal["all"]]] = False,
            link_model: Optional[Any] = None,
            sa_relationship: Optional[RelationshipProperty] = None,  # type: ignore
            sa_relationship_args: Optional[Sequence[Any]] = None,
            sa_relationship_kwargs: Optional[Mapping[str, Any]] = None,
        ) -> None:
            if sa_relationship is not None:
                if sa_relationship_args is not None:
                    raise RuntimeError(
                        "Passing sa_relationship_args is not supported when "
                        "also passing a sa_relationship"
                    )
                if sa_relationship_kwargs is not None:
                    raise RuntimeError(
                        "Passing sa_relationship_kwargs is not supported when "
                        "also passing a sa_relationship"
                    )
            self.back_populates = back_populates
            self.cascade_delete = cascade_delete
            self.passive_deletes = passive_deletes
            self.link_model = link_model
            self.sa_relationship = sa_relationship
            self.sa_relationship_args = sa_relationship_args
            self.sa_relationship_kwargs = sa_relationship_kwargs


"""
mongo imports; and their defaults if the 'beanie' package is not installed
"""
try:
    from beanie import Document, PydanticObjectId, SortDirection, init_beanie
    from motor.motor_asyncio import (
        AsyncIOMotorClient,
        AsyncIOMotorClientSession,
        AsyncIOMotorCollection,
    )
except ImportError:
    from pydantic import BaseModel

    init_beanie = lambda *a, **k: dict(**k)
    SortDirection = Any
    AsyncIOMotorClient = lambda *a, **k: dict(**k)
    AsyncIOMotorClientSession = Any
    AsyncIOMotorCollection = Any
    PydanticObjectId = Any
    Document = BaseModel
