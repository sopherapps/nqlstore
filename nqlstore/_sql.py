"""SQL implementation"""

import copy
import sys
from collections.abc import Mapping, MutableMapping
from typing import Any, Dict, Iterable, Literal, Union

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


class _SQLModelMeta(_SQLModel):
    """The base class for all SQL models"""

    id: int | None = Field(default=None, primary_key=True)
    __rel_field_cache__: dict = {}
    """dict of (name, Field) that have associated relationships"""

    @classmethod
    @property
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
        relations_mappers = self.__class__.__relational_fields__
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
        relations_mapper = model.__relational_fields__

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
                    embedded_value = _embed_related_value(parent, field, raw_value)

                    if isinstance(embedded_value, _SQLModel):
                        embedded_values.append(embedded_value)
                    elif isinstance(embedded_value, Iterable):
                        embedded_values += embedded_value

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

            embedded_updates = _get_relational_updates(model, updates)
            result_ids = [v.id for v in results]
            insert_func = await _get_insert(session)
            relations_mapper = model.__relational_fields__
            for k, v in embedded_updates.items():
                field = relations_mapper[k]
                field_props = field.property
                field_model = field_props.mapper.class_
                link_model = model.__sqlmodel_relationships__[k].link_model

                # fk means foreign key
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
                # FIXME: comment above is unclear
                # get parsed embedded values so that they can replace
                # the old relations.
                # Note: this operation is strictly replace, not patch
                embedded_values = []
                through_table_values = []
                fk_values = []
                for parent in results:
                    embedded_value = _embed_related_value(parent, field, v)
                    initial_embedded_values_len = len(embedded_values)
                    if isinstance(embedded_value, _SQLModel):
                        embedded_values.append(embedded_value)
                    elif isinstance(embedded_value, Iterable):
                        embedded_values += embedded_value

                    if link_model is not None:
                        # FIXME: unclear name 'index_range'
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
    relationships = list(model.__relational_fields__.values())
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
    targets = [v.property.target for v in model.__relational_fields__.values()]
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


def _embed_related_value(
    parent: _SQLModel,
    related_field: Any,
    related_value: Iterable[dict | Any] | dict | Any,
) -> Iterable[_SQLModel] | _SQLModel | None:
    """Embeds a given relationship into the parent in place and returns the related records

    Args:
        parent: the model that contains the given relationships
        related_field: the field that contains the given relationship
        related_value: the values correspond to the related field

    Returns:
        the related record(s)
    """
    if related_value is None:
        return None

    props = related_field.property  # type: RelationshipProperty[Any]
    wrapper_type = props.collection_class
    field_model = props.mapper.class_
    parent_foreign_key_field = props.primaryjoin.right.name
    direction = props.direction

    if direction == RelationshipDirection.MANYTOONE:
        related_value_id_key = props.primaryjoin.left.name
        parent_foreign_key_value = related_value.get(related_value_id_key)
        # update the foreign key value in the parent
        setattr(parent, parent_foreign_key_field, parent_foreign_key_value)
        # create child
        child = field_model.model_validate(related_value)
        # update nested relationships
        for field_name, field_type in field_model.__relational_fields__.items():
            if isinstance(related_value, dict):
                nested_related_value = related_value.get(field_name)
            else:
                nested_related_value = getattr(related_value, field_name)

            nested_related_records = _embed_related_value(
                parent=child,
                related_field=field_type,
                related_value=nested_related_value,
            )
            setattr(child, field_name, nested_related_records)

        return child

    elif direction == RelationshipDirection.ONETOMANY:
        related_value_id_key = props.primaryjoin.left.name
        parent_foreign_key_value = getattr(parent, related_value_id_key)
        # add a foreign key values to link back to parent
        if issubclass(wrapper_type, (list, tuple, set)):
            embedded_records = []
            for v in related_value:
                child = field_model.model_validate(
                    _with_value(v, parent_foreign_key_field, parent_foreign_key_value)
                )

                # update nested relationships
                for field_name, field_type in field_model.__relational_fields__.items():
                    if isinstance(v, dict):
                        nested_related_value = v.get(field_name)
                    else:
                        nested_related_value = getattr(v, field_name)

                    nested_related_records = _embed_related_value(
                        parent=child,
                        related_field=field_type,
                        related_value=nested_related_value,
                    )
                    setattr(child, field_name, nested_related_records)

                embedded_records.append(child)

            return wrapper_type(embedded_records)

    elif direction == RelationshipDirection.MANYTOMANY:
        if issubclass(wrapper_type, (list, tuple, set)):
            embedded_records = []
            for v in related_value:
                child = field_model.model_validate(v)

                # update nested relationships
                for field_name, field_type in field_model.__relational_fields__.items():
                    if isinstance(v, dict):
                        nested_related_value = v.get(field_name)
                    else:
                        nested_related_value = getattr(v, field_name)
                    nested_related_records = _embed_related_value(
                        parent=child,
                        related_field=field_type,
                        related_value=nested_related_value,
                    )
                    setattr(child, field_name, nested_related_records)

                embedded_records.append(child)

            return wrapper_type(embedded_records)

    raise NotImplementedError(
        f"relationship {direction} of type annotation {wrapper_type} not supported yet"
    )


# FIXME: Allow multiple levels of nesting
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

    # FIXME: Maybe check if any relationship value is passed by checking the keys of value
    #   And then do a recursive embedded parse and return the field_model

    if direction == RelationshipDirection.MANYTOONE:
        if any([k in field_model.__relational_fields__ for k in value]):
            # FIXME: nested relationships exist
            pass
        # # add a foreign key value to link back to parent
        return {fk_field: fk_value}, field_model.model_validate(value)

    if direction == RelationshipDirection.ONETOMANY:
        # add a foreign key values to link back to parent
        if issubclass(wrapper_type, (list, tuple, set)):
            return {}, wrapper_type(
                [
                    (
                        field_model.model_validate(_with_value(v, fk_field, fk_value))
                        # FIXME: Add a proper call to nested recursion for all relational_fields
                        if any([k in field_model.__relational_fields__ for k in v])
                        else field_model.model_validate(
                            _with_value(v, fk_field, fk_value)
                        )
                    )
                    for v in value
                ]
            )

    if direction == RelationshipDirection.MANYTOMANY:
        if issubclass(wrapper_type, (list, tuple, set)):
            return {}, wrapper_type(
                [
                    (
                        field_model.model_validate(v)
                        # FIXME: Add a proper call to nested recursion for all relational_fields
                        if any([k in field_model.__relational_fields__ for k in v])
                        else field_model.model_validate(v)
                    )
                    for v in value
                ]
            )

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


def _get_relational_updates(model: type[_SQLModelMeta], updates: dict) -> dict:
    """Gets the updates that are affect only the relationships on this model

    Args:
        model: the model to be updated
        updates: the dict of new values to updated on the matched records

    Returns:
        a dict with only updates concerning the relationships of the given model
    """
    return {k: v for k, v in updates.items() if k in model.__relational_fields__}


def _get_non_relational_updates(model: type[_SQLModelMeta], updates: dict) -> dict:
    """Gets the updates that do not affect relationships on this model

    Args:
        model: the model to be updated
        updates: the dict of new values to updated on the matched records

    Returns:
        a dict with only updates that do not affect relationships on this model
    """
    return {k: v for k, v in updates.items() if k not in model.__relational_fields__}


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
    relations = list(model.__relational_fields__.values())

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
