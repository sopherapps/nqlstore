"""SQL implementation"""

from typing import Any, Iterable, TypeVar

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql._typing import (
    _ColumnExpressionArgument,
    _ColumnExpressionOrStrLabelArgument,
)
from sqlmodel import *
from sqlmodel.ext.asyncio.session import AsyncSession

from ._base import BaseStore
from .query.parsers import QueryParser
from .query.selectors import QuerySelector

_T = TypeVar("_T", bound=SQLModel)
_Filter = _ColumnExpressionArgument[bool] | bool


class SQLStore(BaseStore):
    """The store based on SQL relational database"""

    def __init__(self, uri: str, parser: QueryParser | None = None, **kwargs):
        super().__init__(uri, parser=parser, **kwargs)
        self._engine = create_async_engine(uri, **kwargs)

    async def register(self, models: list[type[_T]], checkfirst: bool = True):
        tables = [v.__table__ for v in models]
        async with self._engine.begin() as conn:
            await conn.run_sync(
                SQLModel.metadata.create_all, tables=tables, checkfirst=checkfirst
            )

    async def insert(
        self, model: type[_T], items: Iterable[_T | dict], **kwargs
    ) -> list[_T]:
        parsed_items = [v if isinstance(v, model) else model(**v) for v in items]
        async with AsyncSession(self._engine) as session:
            stmt = insert(model).returning(model)
            cursor = await session.stream_scalars(stmt, parsed_items)
            await session.commit()
            results = await cursor.all()
            return list(results)

    async def find(
        self,
        model: type[_T],
        *filters: _Filter,
        nql_query: QuerySelector | None = None,
        skip: int = 0,
        limit: int | None = None,
        sort: tuple[_ColumnExpressionOrStrLabelArgument[Any]] = (),
        **kwargs,
    ) -> list[_T]:
        async with AsyncSession(self._engine) as session:
            nql_filters = ()
            if nql_query:
                nql_filters = self._parser.to_sql(model, query=nql_query)

            cursor = await session.stream_scalars(
                select(model)
                .where(*filters, *nql_filters)
                .limit(limit)
                .offset(skip)
                .order_by(*sort)
            )
            results = await cursor.all()
            return list(results)

    async def update(
        self,
        model: type[_T],
        *filters: _Filter,
        nql_query: QuerySelector | None = None,
        updates: dict | None = None,
        **kwargs,
    ) -> list[_T]:
        async with AsyncSession(self._engine) as session:
            nql_filters = ()
            if nql_query:
                nql_filters = self._parser.to_sql(model, query=nql_query)

            stmt = (
                update(model)
                .where(*filters, *nql_filters)
                .values(**updates)
                .returning(model.__table__)
            )
            cursor = await session.stream(stmt)
            results = await cursor.fetchall()
            await session.commit()
            return [model(**row._mapping) for row in results]

    async def delete(
        self,
        model: type[_T],
        *filters: _Filter,
        nql_query: QuerySelector | None = None,
        **kwargs,
    ) -> list[_T]:
        async with AsyncSession(self._engine) as session:
            nql_filters = ()
            if nql_query:
                nql_filters = self._parser.to_sql(model, query=nql_query)

            cursor = await session.stream(
                delete(model).where(*filters, *nql_filters).returning(model.__table__)
            )
            results = await cursor.all()
            await session.commit()
            return [model(**row._mapping) for row in results]
