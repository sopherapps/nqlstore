"""SQL implementation"""

from typing import Iterable, TypeVar

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql._typing import _ColumnExpressionArgument
from sqlmodel import *
from sqlmodel.sql._expression_select_cls import Select
from sqlmodel.ext.asyncio.session import AsyncSession

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
    ) -> Iterable[_T]:
        parsed_items = [v if isinstance(v, model) else model(**v) for v in items]
        async with AsyncSession(self._engine) as session:
            return await session.scalars(insert(model).returning(model), parsed_items)

    async def find(
        self,
        model: type[_T],
        *filters: _Filter,
        skip: int = 0,
        limit: int | None = None,
        **kwargs,
    ) -> Iterable[_T]:
        async with AsyncSession(self._engine) as session:
            statement: Select = (  # noqa
                select(model).where(*filters).limit(limit).offset(skip)
            )
            results = await session.exec(statement)
        return results

    async def update(
        self, model: type[_T], *filters: _Filter, updates: dict, **kwargs
    ) -> Iterable[_T]:
        async with AsyncSession(self._engine) as session:
            return await session.scalars(
                update(model).where(*filters).values(**updates).returning(model)
            )

    async def delete(
        self, model: type[_T], *filters: _Filter, **kwargs
    ) -> Iterable[_T]:
        async with AsyncSession(self._engine) as session:
            return await session.scalars(delete(model).where(*filters).returning(model))
