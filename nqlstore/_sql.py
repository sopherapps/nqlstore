"""SQL implementation"""

import copy
import sys
from collections.abc import Mapping, MutableMapping
from typing import Any, Dict, Iterable, Literal, TypeVar, Union

from pydantic import create_model
from pydantic.main import ModelT

from ._base import BaseStore
from ._compat import (
    AsyncSession,
    Column,
    DetachedInstanceError,
    IncEx,
    InstrumentedAttribute,
    RelationshipDirection,
    RelationshipProperty,
    Table,
    _ColumnExpressionArgument,
    _ColumnExpressionOrStrLabelArgument,
    _SQLModel,
    create_async_engine,
    delete,
    insert,
    pg_insert,
    select,
    sqlite_insert,
    subqueryload,
    update,
    func,
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
        tables = [v.__table__ for v in models if hasattr(v, "__table__")]
        async with self._engine.begin() as conn:
            await conn.run_sync(
                _SQLModel.metadata.create_all, tables=tables, checkfirst=checkfirst
            )

    async def insert(
        self, model: type[_T], items: Iterable[_T | dict], **kwargs
    ) -> list[_T]:
        parsed_items = [
            v if isinstance(v, model) else model.model_validate(v) for v in items
        ]
        relations_mapper = _get_relations_mapper(model)

        async with AsyncSession(self._engine) as session:
            insert_func = await _get_insert(session)
            stmt = insert_func(model).returning(model)
            cursor = await session.stream_scalars(stmt, parsed_items)
            results = await cursor.all()
            result_ids = [v.id for v in results]

            # insert embedded items also to permit something like
            # store.insert(Lib, [{"books": [{"title": "yay"}, ...]}])
            # where "books" is a one-to-many relationship
            # i.e. the kind that might be 'embedded' in Mongo-terms
            for k, field in relations_mapper.items():
                embedded_values = []

                for idx, record in enumerate(items):
                    parent = results[idx]
                    raw_value = _get_key_or_prop(record, k)
                    parent_partial, embedded_value = _parse_embedded(
                        raw_value, field, parent
                    )
                    if isinstance(embedded_value, _SQLModel):
                        embedded_values.append(embedded_value)
                    elif isinstance(embedded_value, Iterable):
                        embedded_values += embedded_value

                    # for many-to-one relationships, the parent
                    # also needs to be updated.
                    # add the partial update of the parent
                    for key, val in parent_partial.items():
                        setattr(parent, key, val)

                # insert the related items
                if len(embedded_values) > 0:
                    field_model = field.property.mapper.class_

                    try:
                        # PostgreSQL and SQLite support on_conflict_do_nothing
                        embed_stmt = (
                            insert_func(field_model)
                            .on_conflict_do_nothing()
                            .returning(field_model)
                        )
                    except AttributeError:
                        # MySQL supports prefix("IGNORE")
                        # Other databases might fail at this point
                        embed_stmt = (
                            insert_func(field_model)
                            .prefix_with("IGNORE", dialect="mysql")
                            .returning(field_model)
                        )

                    await session.stream_scalars(embed_stmt, embedded_values)

            # update the updated parents
            session.add_all(results)

            await session.commit()
            refreshed_results = await self.find(model, model.id.in_(result_ids))
            return list(refreshed_results)

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
        updates = copy.deepcopy(updates)
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

            # dealing with nested models in the update
            relations_mapper = _get_relations_mapper(model)
            embedded_updates = {}
            for k in relations_mapper:
                try:
                    embedded_updates[k] = updates.pop(k)
                except KeyError:
                    pass

            if len(updates) > 0:
                stmt = (
                    update(model)
                    .where(*non_rel_filters, *rel_filters)
                    .values(**updates)
                    .returning(model)
                )

                cursor = await session.stream_scalars(stmt)
                results = await cursor.fetchall()
                result_ids = [v.id for v in results]
            else:
                results = await self.find(model, *filters)
                result_ids = [v.id for v in results]

            insert_func = await _get_insert(session)

            for k, v in embedded_updates.items():
                field = relations_mapper[k]
                field_props = field.property
                field_model = field_props.mapper.class_
                link_model = model.__sqlmodel_relationships__[k].link_model

                # fk = foreign key
                if link_model is not None:
                    child_id_field_name = field_props.secondaryjoin.left.name
                    parent_id_field_name = field_props.primaryjoin.left.name
                    child_fk_field_name = field_props.secondaryjoin.right.name
                    parent_fk_field_name = field_props.primaryjoin.right.name

                else:
                    parent_id_field_name = field_props.primaryjoin.left.name
                    child_fk_field_name = field_props.primaryjoin.right.name
                    fk_field = getattr(field_model, child_fk_field_name)

                # get the foreign keys to use in resetting all affected
                # relationships;
                # get parsed embedded values so that they can replace
                # the old relations.
                # Note: this operation is strictly replace, not patch
                embedded_values = []
                through_table_values = []
                fk_values = []
                for parent in results:
                    parent_partial, embedded_value = _parse_embedded(v, field, parent)
                    initial_embedded_values_len = len(embedded_values)
                    if isinstance(embedded_value, _SQLModel):
                        embedded_values.append(embedded_value)
                    elif isinstance(embedded_value, Iterable):
                        embedded_values += embedded_value

                    if link_model is not None:
                        index_range = (
                            initial_embedded_values_len,
                            len(embedded_values),
                        )
                        through_table_values.append(
                            {
                                parent_fk_field_name: getattr(
                                    parent, parent_id_field_name
                                ),
                                "index_range": index_range,
                            }
                        )
                    else:
                        fk_values.append(getattr(parent, parent_id_field_name))

                    # for many-to-one relationships, the parent
                    # also needs to be updated.
                    # add the partial update of the parent
                    for key, val in parent_partial.items():
                        setattr(parent, key, val)

                if len(embedded_values) > 0:
                    # Reset the relationship; delete all other related items
                    # Currently, this operation replaces all past relations
                    if fk_field is not None and len(fk_values) > 0:
                        reset_stmt = delete(field_model).where(fk_field.in_(fk_values))
                        await session.stream(reset_stmt)

                    # insert the embedded items
                    try:
                        # PostgreSQL and SQLite support on_conflict_do_nothing
                        embed_stmt = (
                            insert_func(field_model)
                            .on_conflict_do_nothing()
                            .returning(field_model)
                        )
                    except AttributeError:
                        # MySQL supports prefix("IGNORE")
                        # Other databases might fail at this point
                        embed_stmt = (
                            insert_func(field_model)
                            .prefix_with("IGNORE", dialect="mysql")
                            .returning(field_model)
                        )

                    embedded_cursor = await session.stream_scalars(
                        embed_stmt, embedded_values
                    )
                    embedded_results = await embedded_cursor.all()

                    if len(through_table_values) > 0:
                        parent_fk_values = [
                            v[parent_fk_field_name] for v in through_table_values
                        ]
                        if len(parent_fk_values) > 0:
                            # Reset the relationship; delete all other related items
                            # Currently, this operation replaces all past relations
                            parent_fk_field = getattr(link_model, parent_id_field_name)
                            reset_stmt = delete(link_model).where(
                                parent_fk_field.in_(parent_fk_values)
                            )
                            await session.stream(reset_stmt)

                        # insert the through table records
                        try:
                            # PostgreSQL and SQLite support on_conflict_do_nothing
                            through_table_stmt = (
                                insert_func(link_model)
                                .on_conflict_do_nothing()
                                .returning(link_model)
                            )
                        except AttributeError:
                            # MySQL supports prefix("IGNORE")
                            # Other databases might fail at this point
                            through_table_stmt = (
                                insert_func(link_model)
                                .prefix_with("IGNORE", dialect="mysql")
                                .returning(link_model)
                            )

                        # compute the next id auto-incremented
                        next_id = await session.scalar(func.max(link_model.id))
                        next_id = (next_id or 0) + 1

                        through_table_values = [
                            {
                                parent_fk_field_name: v[parent_fk_field_name],
                                child_fk_field_name: getattr(
                                    child, child_id_field_name
                                ),
                            }
                            for v in through_table_values
                            for child in embedded_results[
                                v["index_range"][0] : v["index_range"][1]
                            ]
                        ]
                        through_table_values = [
                            link_model(id=next_id + idx, **v)
                            for idx, v in enumerate(through_table_values)
                        ]
                        await session.stream_scalars(
                            through_table_stmt, through_table_values
                        )

            # update the updated parents
            session.add_all(results)

            await session.commit()
            refreshed_results = await self.find(model, model.id.in_(result_ids))
            return list(refreshed_results)

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

            deleted_items = await self.find(model, *filters)

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

            await session.stream(
                delete(model)
                .where(*non_rel_filters, *rel_filters)
                .execution_options(**exec_options),
            )
            await session.commit()
            return deleted_items


class _SQLModelMeta(_SQLModel):
    """The base class for all SQL models"""

    id: int | None = Field(default=None, primary_key=True)

    def model_dump(
        self,
        *,
        mode: Union[Literal["json", "python"], str] = "python",
        include: IncEx = None,
        exclude: IncEx = None,
        context: Union[Dict[str, Any], None] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        round_trip: bool = False,
        warnings: Union[bool, Literal["none", "warn", "error"]] = True,
        serialize_as_any: bool = False,
    ) -> Dict[str, Any]:
        data = super().model_dump(
            mode=mode,
            include=include,
            exclude=exclude,
            context=context,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            round_trip=round_trip,
            warnings=warnings,
            serialize_as_any=serialize_as_any,
        )
        relations_mappers = _get_relations_mapper(self.__class__)
        for k, field in relations_mappers.items():
            if exclude is None or k not in exclude:
                try:
                    value = getattr(self, k, None)
                except DetachedInstanceError:
                    # ignore lazy loaded values
                    continue

                if value is not None or not exclude_none:
                    data[k] = _serialize_embedded(
                        value,
                        field=field,
                        mode=mode,
                        context=context,
                        by_alias=by_alias,
                        exclude_unset=exclude_unset,
                        exclude_defaults=exclude_defaults,
                        exclude_none=exclude_none,
                        round_trip=round_trip,
                        warnings=warnings,
                        serialize_as_any=serialize_as_any,
                    )

        return data


def SQLModel(
    name: str,
    schema: type[ModelT],
    /,
    relationships: dict[str, type[Any] | type[Union[Any]]] = None,
    link_models: dict[str, type[Any]] = None,
    table: bool = True,
    **kwargs: Any,
) -> type[_SQLModelMeta] | type[ModelT]:
    """Creates a new SQLModel for the given schema for redis

    A new model can be defined by::

        Model = SQLModel("Model", Schema)

    Args:
        name: the name of the model
        schema: the schema from which the model is to be made
        relationships: a map of <name>:annotation for all relationships
        link_models: a map of <field name>:Model class for all link (through)
            tables in many-to-many relationships
        table: whether this model should have a table in the database or not;
            default = True
        kwargs: key-word args to pass to the SQLModel when defining it

    Returns:
        a SQLModel model class with the given name
    """
    fields = get_field_definitions(
        schema, relationships=relationships, link_models=link_models, is_for_sql=True
    )

    return create_model(
        name,
        # module of the calling function
        __module__=sys._getframe(1).f_globals["__name__"],
        __doc__=schema.__doc__,
        __cls_kwargs__={"table": table, **kwargs},
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


def _get_relations_mapper(model: type[_SQLModel]) -> dict[str, Any]:
    """Gets all the relational fields with their names of the given model

    Args:
        model: the SQL model to inspect

    Returns:
        dict of (name, Field) that have associated relationships
    """
    return {
        k: v
        for k, v in model.__mapper__.all_orm_descriptors.items()
        if isinstance(v.property, RelationshipProperty)
    }


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


def _get_key_or_prop(obj: dict | Any, name: str) -> Any:
    """Gets value of a key or property from a given object or dict

    Args:
        obj: the object from which to get the value
        name: the name of the key or property

    Returns:
        the value corresponding to the given key or property
    """
    if isinstance(obj, Mapping):
        return obj.get(name, None)
    else:
        return getattr(obj, name, None)


def _with_value(obj: dict | Any, field: str, value: Any) -> Any:
    """Sets the value of a key or property of a given object or dict

    Note: this mutates the object in-place

    Args:
        obj: the object
        field: the name of the key or property
        value: the value to set

    Returns:
        the mutated object
    """
    if isinstance(obj, MutableMapping):
        obj[field] = value
    else:
        setattr(obj, field, value)

    return obj


def _parse_embedded(
    value: Iterable[dict | Any] | dict | Any, field: Any, parent: _SQLModel
) -> tuple[dict, Iterable[_SQLModel] | _SQLModel | None]:
    """Parses embedded items that can be a single item or many into SQLModels

    Args:
        value: the value to parse
        field: the field on which these embedded items are
        parent: the parent SQLModel to which this value is attached

    Returns:
        tuple (parent_partial, embedded_models): where parent_partial is the partial update of the parent
            and embedded_models is an iterable of SQLModel instances or a single SQLModel instance
            or None if value is None
    """
    if value is None:
        return {}, None

    props = field.property  # type: RelationshipProperty[Any]
    wrapper_type = props.collection_class
    field_model = props.mapper.class_
    fk_field = props.primaryjoin.right.name
    parent_id_field = props.primaryjoin.left.name
    fk_value = getattr(parent, parent_id_field)
    direction = props.direction

    if direction == RelationshipDirection.MANYTOONE:
        # # add a foreign key value to link back to parent
        return {fk_field: fk_value}, field_model.model_validate(value)

    if direction == RelationshipDirection.ONETOMANY:
        # add a foreign key values to link back to parent
        if issubclass(wrapper_type, (list, tuple, set)):
            return {}, wrapper_type(
                [
                    field_model.model_validate(_with_value(v, fk_field, fk_value))
                    for v in value
                ]
            )

    if direction == RelationshipDirection.MANYTOMANY:
        # add a foreign key values to link back to parent
        if issubclass(wrapper_type, (list, tuple, set)):
            return {}, wrapper_type([field_model.model_validate(v) for v in value])

    raise NotImplementedError(
        f"relationship {direction} of type annotation {wrapper_type} not supported yet"
    )


def _serialize_embedded(
    value: Iterable[_SQLModel] | _SQLModel, field: Any, **kwargs
) -> Iterable[dict] | dict | None:
    """Serializes embedded items that can be a single item or many into SQLModels

    Args:
        value: the value to serialize
        field: the field on which these embedded items are
        kwargs: extra key-word args to pass to model_dump()

    Returns:
        An iterable of dicts or a single dict
        or None if value is None
    """
    if value is None:
        return None

    props = field.property  # type: RelationshipProperty[Any]
    wrapper_type = props.collection_class

    if wrapper_type is None:
        return value.model_dump(**kwargs)
    elif issubclass(wrapper_type, (list, tuple, set)):
        # add a foreign key values to link back to parent
        return wrapper_type([v.model_dump(**kwargs) for v in value])

    raise NotImplementedError(
        f"relationship of type annotation {wrapper_type} not supported yet"
    )


async def _get_insert(session: AsyncSession):
    """Gets the insert statement for the given session

    Args:
        session: the async session connecting to the database

    Returns:
        the insert function
    """
    conn = await session.connection()
    dialect = conn.dialect
    dialect_name = dialect.name

    if dialect_name == "sqlite":
        return sqlite_insert
    if dialect_name == "postgresql":
        return pg_insert

    return insert
