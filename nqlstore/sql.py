"""SQL implementation"""

from typing import Any, AsyncIterable, Iterable, TypeVar

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql._typing import (
    _ColumnExpressionArgument,
    _ColumnExpressionOrStrLabelArgument,
)
from sqlmodel import *
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel.sql._expression_select_cls import Select

from ._base import BaseStore

_T = TypeVar("_T", bound=SQLModel)
_Filter = _ColumnExpressionArgument[bool] | bool


class SQLStore(BaseStore):
    """The store based on SQL relational database"""

    def __init__(self, uri: str, **kwargs):
        super().__init__(uri, **kwargs)
        self._engine = create_async_engine(uri, **kwargs)

    async def register(self, models: list[type[_T]], checkfirst: bool = True):
        tables = [v.__table__ for v in models]
        async with self._engine.begin() as conn:
            await conn.run_sync(
                SQLModel.metadata.create_all, tables=tables, checkfirst=checkfirst
            )

    async def insert(
        self, model: type[_T], items: Iterable[_T | dict], **kwargs
    ) -> AsyncIterable[_T]:
        parsed_items = [v if isinstance(v, model) else model(**v) for v in items]
        async with AsyncSession(self._engine) as session:
            return await session.stream_scalars(
                insert(model).returning(model), parsed_items
            )

    async def find(
        self,
        model: type[_T],
        *filters: _Filter,
        skip: int = 0,
        limit: int | None = None,
        sort: tuple[_ColumnExpressionOrStrLabelArgument[Any]] = (),
        **kwargs,
    ) -> AsyncIterable[_T]:
        async with AsyncSession(self._engine) as session:
            return await session.stream_scalars(
                select(model).where(*filters).limit(limit).offset(skip).order_by(*sort)
            )

    async def update(
        self, model: type[_T], *filters: _Filter, updates: dict, **kwargs
    ) -> AsyncIterable[_T]:
        async with AsyncSession(self._engine) as session:
            return await session.stream_scalars(
                update(model).where(*filters).values(**updates).returning(model)
            )

    async def delete(
        self, model: type[_T], *filters: _Filter, **kwargs
    ) -> AsyncIterable[_T]:
        async with AsyncSession(self._engine) as session:
            return await session.stream_scalars(
                delete(model).where(*filters).returning(model)
            )
