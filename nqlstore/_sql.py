"""SQL implementation"""

import copy
import sys
from collections.abc import Mapping, MutableMapping
from typing import Any, Dict, Iterable, Literal, Sequence, TypeVar, Union

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
    func,
    insert,
    pg_insert,
    select,
    sqlite_insert,
    subqueryload,
    update,
)
from ._field import Field, get_field_definitions
from .query.parsers import QueryParser
from .query.selectors import QuerySelector

_Filter = _ColumnExpressionArgument[bool] | bool
_T = TypeVar("_T")


class _SQLModelMeta(_SQLModel):
    """The base class for all SQL models"""

    id: int | None = Field(default=None, primary_key=True)
    __rel_field_cache__: dict = {}
    """dict of (name, Field) that have associated relationships"""

    @classmethod
    def __relational_fields__(cls) -> dict[str, Any]:
        """dict of (name, Field) that have associated relationships"""

        cls_fullname = f"{cls.__module__}.{cls.__qualname__}"
        try:
            return cls.__rel_field_cache__[cls_fullname]
        except KeyError:
            value = {
                k: v
                for k, v in cls.__mapper__.all_orm_descriptors.items()
                if isinstance(v.property, RelationshipProperty)
            }
            cls.__rel_field_cache__[cls_fullname] = value
            return value

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
        relations_mappers = self.__class__.__relational_fields__()
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


class SQLStore(BaseStore):
    """The store based on SQL relational database"""

    def __init__(self, uri: str, parser: QueryParser | None = None, **kwargs):
        super().__init__(uri, parser=parser, **kwargs)
        self._engine = create_async_engine(uri, **kwargs)

    async def register(
        self, models: list[type[_SQLModelMeta]], checkfirst: bool = True
    ):
        tables = [v.__table__ for v in models if hasattr(v, "__table__")]
        async with self._engine.begin() as conn:
            await conn.run_sync(
                _SQLModel.metadata.create_all, tables=tables, checkfirst=checkfirst
            )

    async def insert(
        self,
        model: type[_SQLModelMeta],
        items: Iterable[_SQLModelMeta | dict],
        **kwargs,
    ) -> list[_SQLModelMeta]:
        parsed_items = [
            v if isinstance(v, model) else model.model_validate(v) for v in items
        ]
        relations_mapper = model.__relational_fields__()

        async with AsyncSession(self._engine) as session:
            insert_stmt = await _get_insert_func(session, model=model)
            cursor = await session.stream_scalars(insert_stmt, parsed_items)
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
                    embedded_value = _embed_value(parent, field, raw_value)

                    if isinstance(embedded_value, _SQLModel):
                        embedded_values.append(embedded_value)
                    elif isinstance(embedded_value, Iterable):
                        embedded_values += embedded_value

                # insert the related items
                if len(embedded_values) > 0:
                    field_model = field.property.mapper.class_
                    embed_stmt = await _get_insert_func(session, model=field_model)
                    await session.stream_scalars(embed_stmt, embedded_values)

            # update the updated parents
            session.add_all(results)

            await session.commit()
            refreshed_results = await self.find(model, model.id.in_(result_ids))
            return list(refreshed_results)

    async def find(
        self,
        model: type[_SQLModelMeta],
        *filters: _Filter,
        query: QuerySelector | None = None,
        skip: int = 0,
        limit: int | None = None,
        sort: tuple[_ColumnExpressionOrStrLabelArgument[Any]] = (),
        **kwargs,
    ) -> list[_SQLModelMeta]:
        async with AsyncSession(self._engine) as session:
            if query:
                filters = (*filters, *self._parser.to_sql(model, query=query))
            return await _find(
                session, model, *filters, skip=skip, limit=limit, sort=sort
            )

    async def update(
        self,
        model: type[_SQLModelMeta],
        *filters: _Filter,
        query: QuerySelector | None = None,
        updates: dict | None = None,
        **kwargs,
    ) -> list[_SQLModelMeta]:
        updates = copy.deepcopy(updates)
        async with AsyncSession(self._engine) as session:
            if query:
                filters = (*filters, *self._parser.to_sql(model, query=query))

            relational_filters = _get_relational_filters(model, filters)
            non_relational_filters = _get_non_relational_filters(model, filters)

            # Let's update the fields that are not embedded model fields
            # and return the affected results
            results = await _update_non_embedded_fields(
                session,
                model,
                *non_relational_filters,
                *relational_filters,
                updates=updates,
            )
            result_ids = [v.id for v in results]

            # Let's update the embedded fields also
            await _update_embedded_fields(
                session, model=model, records=results, updates=updates
            )
            await session.commit()

            refreshed_results = await self.find(model, model.id.in_(result_ids))
            return refreshed_results

    async def delete(
        self,
        model: type[_SQLModelMeta],
        *filters: _Filter,
        query: QuerySelector | None = None,
        **kwargs,
    ) -> list[_SQLModelMeta]:
        async with AsyncSession(self._engine) as session:
            if query:
                filters = (*filters, *self._parser.to_sql(model, query=query))

            deleted_items = await self.find(model, *filters)

            relational_filters = _get_relational_filters(model, filters)
            non_relational_filters = _get_non_relational_filters(model, filters)

            exec_options = {}
            if len(relational_filters) > 0:
                exec_options = {"is_delete_using": True}

            await session.stream(
                delete(model)
                .where(*non_relational_filters, *relational_filters)
                .execution_options(**exec_options),
            )
            await session.commit()
            return deleted_items


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


def _get_relational_filters(
    model: type[_SQLModelMeta],
    filters: Iterable[_Filter],
) -> list[_Filter]:
    """Gets the filters that are concerned with relationships on this model

    The filters returned are in subquery form since 'update' and 'delete'
    in sqlalchemy do not have join and the only way to attach these filters
    to the model is through sub queries

    Args:
        model: the model under consideration
        filters: the tuple of filters to inspect

    Returns:
        list of filters that are concerned with relationships on this model
    """
    relationships = list(model.__relational_fields__().values())
    targets = [v.property.target for v in relationships]
    plain_filters = [
        item
        for item in filters
        if any([getattr(v, "table", None) in targets for v in item.get_children()])
    ]
    return _to_subquery_based_filters(model, plain_filters, relationships)


def _get_non_relational_filters(
    model: type[_SQLModelMeta], filters: Iterable[_Filter]
) -> list[_Filter]:
    """Gets the filters that are NOT concerned with relationships on this model

    Args:
        model: the model under consideration
        filters: the tuple of filters to inspect

    Returns:
        list of filters that are NOT concerned with relationships on this model
    """
    targets = [v.property.target for v in model.__relational_fields__().values()]
    return [
        item
        for item in filters
        if not any([getattr(v, "table", None) in targets for v in item.get_children()])
    ]


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


def _embed_value(
    parent: _SQLModel,
    relationship: Any,
    value: Iterable[dict | Any] | dict | Any,
) -> Iterable[_SQLModel] | _SQLModel | None:
    """Embeds in place a given value into the parent basing on the given relationship

    Note that the parent itself is changed to include the value

    Args:
        parent: the model that contains the given relationships
        relationship: the given relationship
        value: the values correspond to the related field

    Returns:
        the embedded record(s)
    """
    if value is None:
        return None

    props = relationship.property  # type: RelationshipProperty[Any]
    wrapper_type = props.collection_class
    relationship_model = props.mapper.class_
    parent_foreign_key_field = props.primaryjoin.right.name
    direction = props.direction

    if direction == RelationshipDirection.MANYTOONE:
        related_value_id_key = props.primaryjoin.left.name
        parent_foreign_key_value = value.get(related_value_id_key)
        # update the foreign key value in the parent
        setattr(parent, parent_foreign_key_field, parent_foreign_key_value)
        # create child
        child = relationship_model.model_validate(value)
        # update nested relationships
        for (
            field_name,
            field_type,
        ) in relationship_model.__relational_fields__().items():
            if isinstance(value, dict):
                nested_related_value = value.get(field_name)
            else:
                nested_related_value = getattr(value, field_name)

            nested_related_records = _embed_value(
                parent=child, relationship=field_type, value=nested_related_value
            )
            setattr(child, field_name, nested_related_records)

        return child

    elif direction == RelationshipDirection.ONETOMANY:
        related_value_id_key = props.primaryjoin.left.name
        parent_foreign_key_value = getattr(parent, related_value_id_key)
        # add a foreign key values to link back to parent
        if issubclass(wrapper_type, (list, tuple, set)):
            embedded_records = []
            for v in value:
                child = relationship_model.model_validate(
                    _with_value(v, parent_foreign_key_field, parent_foreign_key_value)
                )

                # update nested relationships
                for (
                    field_name,
                    field_type,
                ) in relationship_model.__relational_fields__().items():
                    if isinstance(v, dict):
                        nested_related_value = v.get(field_name)
                    else:
                        nested_related_value = getattr(v, field_name)

                    nested_related_records = _embed_value(
                        parent=child,
                        relationship=field_type,
                        value=nested_related_value,
                    )
                    setattr(child, field_name, nested_related_records)

                embedded_records.append(child)

            return wrapper_type(embedded_records)

    elif direction == RelationshipDirection.MANYTOMANY:
        if issubclass(wrapper_type, (list, tuple, set)):
            embedded_records = []
            for v in value:
                child = relationship_model.model_validate(v)

                # update nested relationships
                for (
                    field_name,
                    field_type,
                ) in relationship_model.__relational_fields__().items():
                    if isinstance(v, dict):
                        nested_related_value = v.get(field_name)
                    else:
                        nested_related_value = getattr(v, field_name)
                    nested_related_records = _embed_value(
                        parent=child,
                        relationship=field_type,
                        value=nested_related_value,
                    )
                    setattr(child, field_name, nested_related_records)

                embedded_records.append(child)

            return wrapper_type(embedded_records)

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


async def _get_insert_func(session: AsyncSession, model: type[_SQLModelMeta]):
    """Gets the insert statement for the given session

    Args:
        session: the async session connecting to the database
        model: the model for which the insert statement is to be obtained

    Returns:
        the insert function
    """
    conn = await session.connection()
    dialect = conn.dialect
    dialect_name = dialect.name

    native_insert_func = insert

    if dialect_name == "sqlite":
        native_insert_func = sqlite_insert
    if dialect_name == "postgresql":
        native_insert_func = pg_insert

    # insert the embedded items
    try:
        # PostgreSQL and SQLite support on_conflict_do_nothing
        return native_insert_func(model).on_conflict_do_nothing().returning(model)
    except AttributeError:
        # MySQL supports prefix("IGNORE")
        # Other databases might fail at this point
        return (
            native_insert_func(model)
            .prefix_with("IGNORE", dialect="mysql")
            .returning(model)
        )


async def _update_non_embedded_fields(
    session: AsyncSession, model: type[_SQLModelMeta], *filters: _Filter, updates: dict
):
    """Updates only the non-embedded fields of the model

    It ignores any relationships and returns the updated results

    Args:
        session: the sqlalchemy session
        model: the model to be updated
        filters: the filters against which to match the records that are to be updated
        updates: the updates to add to each matched record

    Returns:
        the updated records
    """
    non_embedded_updates = _get_non_relational_updates(model, updates)
    if len(non_embedded_updates) == 0:
        # if we supplied an empty update dict to update,
        # there would be an error
        return await _find(session, model, *filters)

    stmt = update(model).where(*filters).values(**non_embedded_updates).returning(model)
    cursor = await session.stream_scalars(stmt)
    return await cursor.fetchall()


async def _update_embedded_fields(
    session: AsyncSession,
    model: type[_SQLModelMeta],
    records: list[_SQLModelMeta],
    updates: dict,
):
    """Updates only the embedded fields of the model for the given records

    It ignores any fields in the `updates` dict that are not for embedded models
    Note: this operation is replaces the values of the embedded fields with the new values
    passed in the `updates` dictionary as opposed to patching the pre-existing values.

    Args:
        session: the sqlalchemy session
        model: the model to be updated
        records: the db records to update
        updates: the updates to add to each record
    """
    embedded_updates = _get_relational_updates(model, updates)
    relations_mapper = model.__relational_fields__()
    for k, v in embedded_updates.items():
        relationship = relations_mapper[k]
        link_model = model.__sqlmodel_relationships__[k].link_model

        # this does a replace operation; i.e. removes old values and replaces them with the updates
        await _bulk_embedded_delete(
            session, relationship=relationship, data=records, link_model=link_model
        )
        await _bulk_embedded_insert(
            session,
            relationship=relationship,
            data=records,
            link_model=link_model,
            payload=v,
        )
    # FIXME: Should the added records be updated with their embedded values?
    # update the updated parents
    session.add_all(records)


async def _bulk_embedded_insert(
    session: AsyncSession,
    relationship: Any,
    data: list[_SQLModelMeta],
    link_model: type[_SQLModelMeta] | None,
    payload: Iterable[dict] | dict,
) -> Sequence[_SQLModelMeta] | None:
    """Inserts the payload into the data following the given relationship

     It updates the database also

    Args:
        session: the database session
        relationship: the relationship the payload has with the data's schema
        link_model: the model for the through table
        payload: the payload to merge into each record in the data

    Returns:
        the updated data including the embedded data in each record
    """
    relationship_props = relationship.property  # type: RelationshipProperty
    relationship_model = relationship_props.mapper.class_

    parsed_embedded_records = [_embed_value(v, relationship, payload) for v in data]

    insert_stmt = await _get_insert_func(session, model=relationship_model)
    embedded_cursor = await session.stream_scalars(
        insert_stmt, _flatten_list(parsed_embedded_records)
    )
    embedded_db_records = await embedded_cursor.all()

    parent_embedded_map = [
        (parent, embedded_db_records[idx : idx + len(_as_list(raw_embedded))])
        for idx, (parent, raw_embedded) in enumerate(zip(data, parsed_embedded_records))
    ]

    # insert through table values
    await _bulk_insert_through_table_data(
        session,
        relationship=relationship,
        link_model=link_model,
        parent_embedded_map=parent_embedded_map,
    )

    return data


async def _bulk_insert_through_table_data(
    session: AsyncSession,
    relationship: Any,
    link_model: type[_SQLModelMeta] | None,
    parent_embedded_map: list[tuple[_SQLModelMeta, list[_SQLModelMeta]]],
):
    """Inserts the link records into the through-table represented by the link_model

    Args:
        session: the database session
        relationship: the relationship the embedded records are based on
        link_model: the model for the through table
        parent_embedded_map: the list of tuples of parent and its associated embedded db records
    """
    if link_model is not None:
        relationship_props = relationship.property  # type: RelationshipProperty
        child_id_field_name = relationship_props.secondaryjoin.left.name
        parent_id_field_name = relationship_props.primaryjoin.left.name
        child_fk_field_name = relationship_props.secondaryjoin.right.name
        parent_fk_field_name = relationship_props.primaryjoin.right.name

        link_raw_values = [
            {
                parent_fk_field_name: getattr(parent, parent_id_field_name),
                child_fk_field_name: getattr(child, child_id_field_name),
            }
            for parent, children in parent_embedded_map
            for child in children
        ]

        next_id = await _get_nextid(session, link_model)
        link_model_values = [
            link_model(id=next_id + idx, **v) for idx, v in enumerate(link_raw_values)
        ]

        insert_stmt = await _get_insert_func(session, model=link_model)
        await session.stream_scalars(insert_stmt, link_model_values)


async def _bulk_embedded_delete(
    session: AsyncSession,
    relationship: Any,
    data: list[SQLModel],
    link_model: type[_SQLModelMeta] | None,
):
    """Deletes the embedded records of the given parent records for the given relationship

    Args:
        session: the database session
        relationship: the relationship whose embedded records are to be deleted for the given records
        link_model: the model for the through table
    """
    relationship_props = relationship.property  # type: RelationshipProperty
    relationship_model = relationship_props.mapper.class_

    parent_id_field_name = relationship_props.primaryjoin.left.name
    parent_foreign_keys = [getattr(item, parent_id_field_name) for item in data]

    if link_model is None:
        reverse_foreign_key_field_name = relationship_props.primaryjoin.right.name
        reverse_foreign_key_field = getattr(
            relationship_model, reverse_foreign_key_field_name
        )
        await session.stream(
            delete(relationship_model).where(
                reverse_foreign_key_field.in_(parent_foreign_keys)
            )
        )
    else:
        reverse_foreign_key_field = getattr(link_model, parent_id_field_name)
        await session.stream(
            delete(link_model).where(reverse_foreign_key_field.in_(parent_foreign_keys))
        )


async def _get_nextid(session: AsyncSession, model: type[_SQLModelMeta]):
    """Gets the next id generator for the given model

    It returns a generator for the auto-incremented integer ID

    Args:
        session: the database session
        model: the model under consideration

    Returns:
        a generator for the auto-incremented integer ID for the given model
    """
    # compute the next id auto-incremented
    next_id = await session.scalar(func.max(model.id))
    next_id = (next_id or 0) + 1
    return next_id


def _flatten_list(data: list[_T | list[_T]]) -> list[_T]:
    """Flattens a list that may have lists of items at some indices

    Args:
        data: the list to flatten

    Returns:
        the flattened list
    """
    results = []
    for item in data:
        if isinstance(item, Iterable) and not isinstance(item, Mapping):
            results += list(item)
        else:
            results.append(item)

    return results


def _as_list(value: Any) -> list:
    """Wraps the value in a list if it is not an iterable

    Args:
        value: the value to wrap in a list if it is not one

    Returns:
        the value as a list if it is not already one
    """
    if isinstance(value, list):
        return value
    elif isinstance(value, Iterable) and not isinstance(value, Mapping):
        return list(value)
    return [value]


def _get_relational_updates(model: type[_SQLModelMeta], updates: dict) -> dict:
    """Gets the updates that are affect only the relationships on this model

    Args:
        model: the model to be updated
        updates: the dict of new values to updated on the matched records

    Returns:
        a dict with only updates concerning the relationships of the given model
    """
    return {k: v for k, v in updates.items() if k in model.__relational_fields__()}


def _get_non_relational_updates(model: type[_SQLModelMeta], updates: dict) -> dict:
    """Gets the updates that do not affect relationships on this model

    Args:
        model: the model to be updated
        updates: the dict of new values to updated on the matched records

    Returns:
        a dict with only updates that do not affect relationships on this model
    """
    return {k: v for k, v in updates.items() if k not in model.__relational_fields__()}


async def _find(
    session: AsyncSession,
    model: type[_SQLModelMeta],
    /,
    *filters: _Filter,
    skip: int = 0,
    limit: int | None = None,
    sort: tuple[_ColumnExpressionOrStrLabelArgument[Any]] = (),
) -> list[_SQLModelMeta]:
    """Finds the records that match the given filters

    Args:
        session: the sqlalchemy session
        model: the model that is to be searched
        filters: the filters to match
        skip: number of records to ignore at the top of the returned results; default is 0
        limit: maximum number of records to return; default is None.
        sort: fields to sort by; default = None

    Returns:
        the records tha match the given filters
    """
    relations = list(model.__relational_fields__().values())

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
