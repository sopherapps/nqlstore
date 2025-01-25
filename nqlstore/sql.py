"""SQL implementation"""
from typing import Iterable, TypeVar

from sqlalchemy import delete, insert, update
from sqlalchemy.sql._typing import _ColumnExpressionArgument
from sqlmodel import Field, Session, SQLModel, create_engine, select
from sqlmodel.sql._expression_select_cls import Select

from ._base import BaseStore

_T = TypeVar("_T", bound=SQLModel)
_Filter = _ColumnExpressionArgument[bool] | bool

__all__ = ["SQLStore", "SQLModel", "Field"]


class SQLStore(BaseStore):
    """The store based on SQL relational database"""

    def __init__(self, uri: str, **kwargs):
        super().__init__(uri, **kwargs)
        self._engine = create_engine(uri, **kwargs)

    def register(self, models: Iterable[type[_T]]):
        tables = [v.__table__ for v in models]
        SQLModel.metadata.create_all(self._engine, tables=tables)

    def insert(
        self, model: type[_T], items: Iterable[_T | dict], **kwargs
    ) -> Iterable[_T]:
        parsed_items = [v if isinstance(v, model) else model(**v) for v in items]
        with Session(self._engine) as session:
            return session.scalars(insert(model).returning(model), parsed_items)

    def find(
        self,
        model: type[_T],
        *filters: _Filter,
        skip: int = 0,
        limit: int | None = None,
    ) -> Iterable[_T]:
        with Session(self._engine) as session:
            statement: Select = (  # noqa
                select(model).where(*filters).limit(limit).offset(skip)
            )
            results = session.exec(statement)
        return results

    def update(self, model: type[_T], *filters: _Filter, updates: dict) -> Iterable[_T]:
        with Session(self._engine) as session:
            return session.scalars(
                update(model).where(*filters).values(**updates).returning(model)
            )

    def delete(self, model: type[_T], *filters: _Filter) -> Iterable[_T]:
        with Session(self._engine) as session:
            return session.scalars(delete(model).where(*filters).returning(model))
