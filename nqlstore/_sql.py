"""SQL implementation"""

from typing import Any, Iterable, TypeVar, Union

from pydantic import create_model
from pydantic.main import ModelT
from sqlalchemy import inspect
from sqlalchemy.orm import joinedload

from ._base import BaseStore
from ._compat import (
    AsyncSession,
    _ColumnExpressionArgument,
    _ColumnExpressionOrStrLabelArgument,
    _SQLModel,
    create_async_engine,
    delete,
    insert,
    select,
    update,
)
from ._field import Field, get_field_definitions
from .query.parsers import QueryParser
from .query.selectors import QuerySelector

_T = TypeVar("_T", bound=_SQLModel)
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
                _SQLModel.metadata.create_all, tables=tables, checkfirst=checkfirst
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
        query: QuerySelector | None = None,
        skip: int = 0,
        limit: int | None = None,
        sort: tuple[_ColumnExpressionOrStrLabelArgument[Any]] = (),
        **kwargs,
    ) -> list[_T]:
        async with AsyncSession(self._engine) as session:
            nql_filters = ()
            if query:
                nql_filters = self._parser.to_sql(model, query=query)

            # eagerly load all relationships so that no validation errors occur due
            # to missing session if there is an attempt to load them lazily later
            rel_options = [joinedload(v) for v in inspect(model).relationships.values()]

            cursor = await session.stream_scalars(
                select(model)
                .where(*filters, *nql_filters)
                .limit(limit)
                .offset(skip)
                .order_by(*sort)
                .options(*rel_options)
            )
            results = await cursor.unique().all()
            return list(results)

    async def update(
        self,
        model: type[_T],
        *filters: _Filter,
        query: QuerySelector | None = None,
        updates: dict | None = None,
        **kwargs,
    ) -> list[_T]:
        async with AsyncSession(self._engine) as session:
            nql_filters = ()
            if query:
                nql_filters = self._parser.to_sql(model, query=query)

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
        query: QuerySelector | None = None,
        **kwargs,
    ) -> list[_T]:
        async with AsyncSession(self._engine) as session:
            nql_filters = ()
            if query:
                nql_filters = self._parser.to_sql(model, query=query)

            cursor = await session.stream(
                delete(model).where(*filters, *nql_filters).returning(model.__table__)
            )
            results = await cursor.all()
            await session.commit()
            return [model(**row._mapping) for row in results]


class _SQLModelMeta(_SQLModel):
    """The base class for all SQL models"""

    id: int | None = Field(default=None, primary_key=True)


def SQLModel(
    name: str,
    schema: type[ModelT],
    /,
    relationships: dict[str, type[Any] | type[Union[Any]]] = None,
) -> type[_SQLModelMeta]:
    """Creates a new SQLModel for the given schema for redis

    A new model can be defined by::

        Model = SQLModel("Model", Schema)

    Args:
        name: the name of the model
        schema: the schema from which the model is to be made
        relationships: a map of <name>:annotation for all relationships

    Returns:
        a SQLModel model class with the given name
    """
    fields = get_field_definitions(schema, relationships=relationships, is_for_sql=True)

    # FIXME: Handle scenario where a pk is defined
    return create_model(
        name,
        __doc__=schema.__doc__,
        __slots__=schema.__slots__,
        __cls_kwargs__={"table": True},
        __base__=(_SQLModelMeta,),
        **fields,
    )
