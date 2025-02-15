"""SQL implementation"""

import sys
from typing import Any, Iterable, TypeVar, Union

from pydantic import create_model
from pydantic.main import ModelT

from ._base import BaseStore
from ._compat import (
    AsyncSession,
    Column,
    InstrumentedAttribute,
    RelationshipProperty,
    Table,
    _ColumnExpressionArgument,
    _ColumnExpressionOrStrLabelArgument,
    _SQLModel,
    create_async_engine,
    delete,
    insert,
    select,
    subqueryload,
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
            if query:
                filters = (*filters, *self._parser.to_sql(model, query=query))

            relations = _get_relations(model)

            # eagerly load all relationships so that no validation errors occur due
            # to missing session if there is an attempt to load them lazily later
            eager_load_opts = [subqueryload(v) for v in relations]

            filtered_relations = _get_filtered_relations(
                filters=filters,
                relations=relations,
            )

            # Note that we need to treat relations that are referenced in the filters
            # differently from those that are not. This is because filtering basing on a relationship
            # requires the use of an inner join. Yet an inner join automatically excludes rows
            # that are have null for a given relationship.
            #
            # An outer join on the other hand would just return all the rows in the left table.
            # We thus need to do an inner join on tables that are being filtered.
            stmt = select(model)
            for rel in filtered_relations:
                stmt = stmt.join_from(model, rel)

            cursor = await session.stream_scalars(
                stmt.options(*eager_load_opts)
                .where(*filters)
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
        query: QuerySelector | None = None,
        updates: dict | None = None,
        **kwargs,
    ) -> list[_T]:
        async with AsyncSession(self._engine) as session:
            if query:
                filters = (*filters, *self._parser.to_sql(model, query=query))

            # Construct filters that have sub queries
            relations = _get_relations(model)
            rel_filters, non_rel_filters = _sieve_rel_from_non_rel_filters(
                filters=filters,
                relations=relations,
            )
            rel_filters = _to_subquery_based_filters(
                model=model,
                rel_filters=rel_filters,
                relations=relations,
            )

            stmt = (
                update(model)
                .where(*non_rel_filters, *rel_filters)
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
            if query:
                filters = (*filters, *self._parser.to_sql(model, query=query))

            # Construct filters that have sub queries
            relations = _get_relations(model)
            rel_filters, non_rel_filters = _sieve_rel_from_non_rel_filters(
                filters=filters,
                relations=relations,
            )
            rel_filters = _to_subquery_based_filters(
                model=model,
                rel_filters=rel_filters,
                relations=relations,
            )
            exec_options = {}
            if len(rel_filters) > 0:
                exec_options = {"is_delete_using": True}

            cursor = await session.stream(
                delete(model)
                .where(*non_rel_filters, *rel_filters)
                .returning(model.__table__)
                .execution_options(**exec_options),
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
) -> type[_SQLModelMeta] | type[ModelT]:
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
        # module of the calling function
        __module__=sys._getframe(1).f_globals["__name__"],
        __doc__=schema.__doc__,
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


def _get_filtered_tables(filters: Iterable[_Filter]) -> list[Table]:
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


def _get_filtered_relations(
    filters: Iterable[_Filter], relations: Iterable[InstrumentedAttribute[Any]]
) -> list[InstrumentedAttribute[Any]]:
    """Retrieves the relations that have been referenced in the filters

    Args:
        filters: the tuple of filters to inspect
        relations: all relations present on the model

    Returns:
        the list of relations referenced in the filters
    """
    filtered_tables = _get_filtered_tables(filters)
    return [rel for rel in relations if rel.property.target in filtered_tables]


def _sieve_rel_from_non_rel_filters(
    filters: Iterable[_Filter], relations: Iterable[InstrumentedAttribute[Any]]
) -> tuple[list[_Filter], list[_Filter]]:
    """Separates relational filters from non-relational ones

    Args:
        filters: the tuple of filters to inspect
        relations: all relations present on the model

    Returns:
        tuple(rel, non_rel) where rel = list of relational filters,
            and non_rel = non-relational filters
    """
    rel_targets = [v.property.target for v in relations]
    rel = []
    non_rel = []

    for filter_ in filters:
        operands = filter_.get_children()
        if any([getattr(v, "table", None) in rel_targets for v in operands]):
            rel.append(filter_)
        else:
            non_rel.append(filter_)

    return rel, non_rel


def _to_subquery_based_filters(
    model: type[_SQLModel],
    rel_filters: list[_Filter],
    relations: Iterable[InstrumentedAttribute[Any]],
) -> list[_Filter]:
    """Converts filters to those that use subqueries to connect to other models

    This is especially important for update() and delete() which do not
    don't have `.join()` methods on them.

    Args:
        model: the model for which the subquery-based filters are to be generated
        rel_filters: the filters that have relationships in them
        relations: the relationship fields on the model

    Returns:
        list of filters that use subqueries to access other tables/models
    """
    # This is based on:
    # https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#update-delete-with-custom-where-criteria-for-joined-table-inheritance
    if len(rel_filters) == 0:
        return []

    filtered_relations = _get_filtered_relations(
        filters=rel_filters,
        relations=relations,
    )

    # create the subquery collecting ids of model, with inner join to related models
    subquery = select(model.id)
    for rel in filtered_relations:
        subquery = subquery.join_from(model, rel)

    # return a filter checking model id against the returned ids
    return [model.id.in_(subquery.where(*rel_filters))]
