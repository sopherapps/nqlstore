"""SQL implementation"""

from typing import Any, Iterable, TypeVar, Union

from pydantic import create_model
from pydantic.main import ModelT
from sqlalchemy import Column, Executable, Table, inspect
from sqlalchemy.orm import (
    InspectionAttr,
    InstrumentedAttribute,
    RelationshipProperty,
    contains_eager,
    joinedload,
    subqueryload,
)

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
from ._field import Field, FieldInfo, RelationshipInfo, get_field_definitions
from .query.parsers import QueryParser
from .query.selectors import QuerySelector

_T = TypeVar("_T", bound=_SQLModel)
_T_stmt = TypeVar("_T_stmt", bound=Executable)
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
            if query:
                filters = (*filters, *self._parser.to_sql(model, query=query))

            relations = _get_relations(model)

            # eagerly load all relationships so that no validation errors occur due
            # to missing session if there is an attempt to load them lazily later
            options = [subqueryload(v) for v in relations]

            stmt = (
                _apply_joins(
                    model,
                    statement=select(model),
                    relations=relations,
                    filters=filters,
                )
                .options(*options)
                .where(*filters)
                .limit(limit)
                .offset(skip)
                .order_by(*sort)
            )

            cursor = await session.stream_scalars(stmt)
            results = await cursor.all()
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
            if query:
                filters = (*filters, *self._parser.to_sql(model, query=query))

            stmt = (
                update(model)
                .where(*filters)
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


def _get_relations(model: type[_SQLModel]):
    """Gets all the relational fields of the given model

    Args:
        model: the SQL model to inspect

    Returns:
        list of Fields that have associated relationships
    """
    return [
        v
        for v in model.__mapper__.all_orm_descriptors.values()
        if isinstance(v.property, RelationshipProperty)
    ]


def _get_filtered_tables(filters: tuple[_Filter, ...]) -> list[Table]:
    """Retrieves the tables that have been referenced in the filters

    Args:
        filters: the tuple of filters to inspect

    Returns:
        the list of Table instances referenced in the filters
    """
    return [
        getattr(v, "table")
        for filter_ in filters
        for v in filter_.get_children()
        if isinstance(v, Column)
    ]


def _apply_joins(
    model: type[_SQLModel],
    statement: _T_stmt,
    relations: list[InstrumentedAttribute[Any]],
    filters: tuple[_Filter, ...],
) -> _T_stmt:
    """Adds join statements to the statement given the relations and the filters

    Note: this changes the stmt in-place

    Args:
        model: the SQLModel class for the given query/mutation
        statement: the executable statement to execute on the session
        relations: the list of relationship fields that this model has
        filters: the tuple of expressions that are being used to match the records in database

    Returns:
        the executable statement with the joins applied
    """
    filtered_tables = _get_filtered_tables(filters)
    filtered_relations = [
        rel for rel in relations if rel.property.target in filtered_tables
    ]

    # Note that we need to treat relations that are referenced in the filters
    # differently from those that are not. This is because filtering basing on a relationship
    # requires the use of an inner join. Yet an inner join automatically excludes rows
    # that are have null for a given relationship.
    #
    # An outer join on the other hand would just return all the rows in the left table.
    # We thus need to do an inner join on tables that are being filtered.
    for rel in filtered_relations:
        statement = statement.join_from(model, rel)

    return statement
