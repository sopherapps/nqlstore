"""The main module containing the default types to be imported"""

from copy import copy
from typing import (
    AbstractSet,
    Any,
    Dict,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Type,
    Union,
    overload,
)

from pydantic.main import ModelT
from pydantic_core import PydanticUndefined as Undefined
from pydantic_core import PydanticUndefinedType as UndefinedType

from ._compat import (
    Column,
    NoArgAnyCallable,
    OnDeleteType,
    RelationshipProperty,
    VectorFieldOptions,
    _RedisFieldInfo,
    _RelationshipInfo,
    _SqlFieldInfo,
    post_init_field_info,
)


class FieldInfo(_SqlFieldInfo, _RedisFieldInfo):
    def __init__(self, default: Any = Undefined, **kwargs: Any) -> None:
        disable_on_redis = kwargs.get("disable_on_redis", False)
        disable_on_sql = kwargs.get("disable_on_sql", False)
        disable_on_mongo = kwargs.get("disable_on_mongo", False)
        super().__init__(default=default, **kwargs)
        self.disable_on_redis = disable_on_redis
        self.disable_on_sql = disable_on_sql
        self.disable_on_mongo = disable_on_mongo


class RelationshipInfo(_RelationshipInfo):
    def __init__(
        self,
        *,
        back_populates: Optional[str] = None,
        cascade_delete: Optional[bool] = False,
        passive_deletes: Optional[Union[bool, Literal["all"]]] = False,
        link_model: Optional[Any] = None,
        sa_relationship: Optional[RelationshipProperty] = None,  # type: ignore
        sa_relationship_args: Optional[Sequence[Any]] = None,
        sa_relationship_kwargs: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ):
        disable_on_redis = kwargs.get("disable_on_redis", False)
        disable_on_sql = kwargs.get("disable_on_sql", False)
        disable_on_mongo = kwargs.get("disable_on_mongo", False)
        default = kwargs.get("default", Undefined)
        super().__init__(
            back_populates=back_populates,
            cascade_delete=cascade_delete,
            passive_deletes=passive_deletes,
            link_model=link_model,
            sa_relationship=sa_relationship,
            sa_relationship_args=sa_relationship_args,
            sa_relationship_kwargs=sa_relationship_kwargs,
        )
        self.disable_on_redis = disable_on_redis
        self.disable_on_sql = disable_on_sql
        self.disable_on_mongo = disable_on_mongo
        self.default = default


# include sa_type, sa_column_args, sa_column_kwargs
@overload
def Field(
    default: Any = Undefined,
    *,
    default_factory: Optional["NoArgAnyCallable"] = None,
    alias: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    exclude: Union[
        AbstractSet[Union[int, str]], Mapping[Union[int, str], Any], Any
    ] = None,
    include: Union[
        AbstractSet[Union[int, str]], Mapping[Union[int, str], Any], Any
    ] = None,
    const: Optional[bool] = None,
    gt: Optional[float] = None,
    ge: Optional[float] = None,
    lt: Optional[float] = None,
    le: Optional[float] = None,
    multiple_of: Optional[float] = None,
    max_digits: Optional[int] = None,
    decimal_places: Optional[int] = None,
    min_items: Optional[int] = None,
    max_items: Optional[int] = None,
    unique_items: Optional[bool] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    allow_mutation: bool = True,
    regex: Optional[str] = None,
    discriminator: Optional[str] = None,
    repr: bool = True,
    primary_key: Union[bool, UndefinedType] = False,
    foreign_key: Any = Undefined,
    unique: Union[bool, UndefinedType] = Undefined,
    nullable: Union[bool, UndefinedType] = Undefined,
    index: Union[bool, UndefinedType] = Undefined,
    sa_type: Union[Type[Any], UndefinedType] = Undefined,
    sa_column_args: Union[Sequence[Any], UndefinedType] = Undefined,
    sa_column_kwargs: Union[Mapping[str, Any], UndefinedType] = Undefined,
    sortable: Union[bool, UndefinedType] = Undefined,
    case_sensitive: Union[bool, UndefinedType] = Undefined,
    full_text_search: Union[bool, UndefinedType] = Undefined,
    vector_options: Optional["VectorFieldOptions"] = None,
    disable_on_redis: bool = False,
    disable_on_sql: bool = False,
    disable_on_mongo: bool = False,
    schema_extra: Optional[Dict[str, Any]] = None,
) -> Any: ...


# When foreign_key is str, include ondelete
# include sa_type, sa_column_args, sa_column_kwargs
@overload
def Field(
    default: Any = Undefined,
    *,
    default_factory: Optional["NoArgAnyCallable"] = None,
    alias: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    exclude: Union[
        AbstractSet[Union[int, str]], Mapping[Union[int, str], Any], Any
    ] = None,
    include: Union[
        AbstractSet[Union[int, str]], Mapping[Union[int, str], Any], Any
    ] = None,
    const: Optional[bool] = None,
    gt: Optional[float] = None,
    ge: Optional[float] = None,
    lt: Optional[float] = None,
    le: Optional[float] = None,
    multiple_of: Optional[float] = None,
    max_digits: Optional[int] = None,
    decimal_places: Optional[int] = None,
    min_items: Optional[int] = None,
    max_items: Optional[int] = None,
    unique_items: Optional[bool] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    allow_mutation: bool = True,
    regex: Optional[str] = None,
    discriminator: Optional[str] = None,
    repr: bool = True,
    primary_key: Union[bool, UndefinedType] = False,
    foreign_key: str,
    ondelete: Union[OnDeleteType, UndefinedType] = Undefined,
    unique: Union[bool, UndefinedType] = Undefined,
    nullable: Union[bool, UndefinedType] = Undefined,
    index: Union[bool, UndefinedType] = Undefined,
    sa_type: Union[Type[Any], UndefinedType] = Undefined,
    sa_column_args: Union[Sequence[Any], UndefinedType] = Undefined,
    sa_column_kwargs: Union[Mapping[str, Any], UndefinedType] = Undefined,
    sortable: Union[bool, UndefinedType] = Undefined,
    case_sensitive: Union[bool, UndefinedType] = Undefined,
    full_text_search: Union[bool, UndefinedType] = Undefined,
    vector_options: Optional["VectorFieldOptions"] = None,
    disable_on_redis: bool = False,
    disable_on_sql: bool = False,
    disable_on_mongo: bool = False,
    schema_extra: Optional[Dict[str, Any]] = None,
) -> Any: ...


# Include sa_column, don't include
# primary_key
# foreign_key
# ondelete
# unique
# nullable
# index
# sa_type
# sa_column_args
# sa_column_kwargs
@overload
def Field(
    default: Any = Undefined,
    *,
    default_factory: Optional["NoArgAnyCallable"] = None,
    alias: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    exclude: Union[
        AbstractSet[Union[int, str]], Mapping[Union[int, str], Any], Any
    ] = None,
    include: Union[
        AbstractSet[Union[int, str]], Mapping[Union[int, str], Any], Any
    ] = None,
    const: Optional[bool] = None,
    gt: Optional[float] = None,
    ge: Optional[float] = None,
    lt: Optional[float] = None,
    le: Optional[float] = None,
    multiple_of: Optional[float] = None,
    max_digits: Optional[int] = None,
    decimal_places: Optional[int] = None,
    min_items: Optional[int] = None,
    max_items: Optional[int] = None,
    unique_items: Optional[bool] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    allow_mutation: bool = True,
    regex: Optional[str] = None,
    discriminator: Optional[str] = None,
    repr: bool = True,
    sa_column: Union["Column", UndefinedType] = Undefined,  # type: ignore
    sortable: Union[bool, UndefinedType] = Undefined,
    case_sensitive: Union[bool, UndefinedType] = Undefined,
    full_text_search: Union[bool, UndefinedType] = Undefined,
    vector_options: Optional["VectorFieldOptions"] = None,
    disable_on_redis: bool = False,
    disable_on_sql: bool = False,
    disable_on_mongo: bool = False,
    schema_extra: Optional[Dict[str, Any]] = None,
) -> Any: ...


def Field(
    default: Any = Undefined,
    *,
    default_factory: Optional["NoArgAnyCallable"] = None,
    alias: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    exclude: Union[
        AbstractSet[Union[int, str]], Mapping[Union[int, str], Any], Any
    ] = None,
    include: Union[
        AbstractSet[Union[int, str]], Mapping[Union[int, str], Any], Any
    ] = None,
    const: Optional[bool] = None,
    gt: Optional[float] = None,
    ge: Optional[float] = None,
    lt: Optional[float] = None,
    le: Optional[float] = None,
    multiple_of: Optional[float] = None,
    max_digits: Optional[int] = None,
    decimal_places: Optional[int] = None,
    min_items: Optional[int] = None,
    max_items: Optional[int] = None,
    unique_items: Optional[bool] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    allow_mutation: bool = True,
    regex: Optional[str] = None,
    discriminator: Optional[str] = None,
    repr: bool = True,
    primary_key: Union[bool, UndefinedType] = False,
    foreign_key: Any = Undefined,
    ondelete: Union[OnDeleteType, UndefinedType] = Undefined,
    unique: Union[bool, UndefinedType] = Undefined,
    nullable: Union[bool, UndefinedType] = Undefined,
    index: Union[bool, UndefinedType] = Undefined,
    sa_type: Union[Type[Any], UndefinedType] = Undefined,
    sa_column: Union["Column", UndefinedType] = Undefined,  # type: ignore
    sa_column_args: Union[Sequence[Any], UndefinedType] = Undefined,
    sa_column_kwargs: Union[Mapping[str, Any], UndefinedType] = Undefined,
    sortable: Union[bool, UndefinedType] = Undefined,
    case_sensitive: Union[bool, UndefinedType] = Undefined,
    full_text_search: Union[bool, UndefinedType] = Undefined,
    vector_options: Optional["VectorFieldOptions"] = None,
    disable_on_redis: bool = False,
    disable_on_sql: bool = False,
    disable_on_mongo: bool = False,
    schema_extra: Optional[Dict[str, Any]] = None,
) -> Any:
    current_schema_extra = schema_extra or {}
    field_info = FieldInfo(
        default,
        default_factory=default_factory,
        alias=alias,
        title=title,
        description=description,
        exclude=exclude,
        include=include,
        const=const,
        gt=gt,
        ge=ge,
        lt=lt,
        le=le,
        multiple_of=multiple_of,
        max_digits=max_digits,
        decimal_places=decimal_places,
        min_items=min_items,
        max_items=max_items,
        unique_items=unique_items,
        min_length=min_length,
        max_length=max_length,
        allow_mutation=allow_mutation,
        regex=regex,
        discriminator=discriminator,
        repr=repr,
        primary_key=primary_key,
        foreign_key=foreign_key,
        ondelete=ondelete,
        unique=unique,
        nullable=nullable,
        index=index,
        sa_type=sa_type,
        sa_column=sa_column,
        sa_column_args=sa_column_args,
        sa_column_kwargs=sa_column_kwargs,
        sortable=sortable,
        case_sensitive=case_sensitive,
        full_text_search=full_text_search,
        vector_options=vector_options,
        disable_on_redis=disable_on_redis,
        disable_on_sql=disable_on_sql,
        disable_on_mongo=disable_on_mongo,
        **current_schema_extra,
    )
    post_init_field_info(field_info)
    return field_info


@overload
def Relationship(
    *,
    back_populates: Optional[str] = None,
    cascade_delete: Optional[bool] = False,
    passive_deletes: Optional[Union[bool, Literal["all"]]] = False,
    link_model: Optional[Any] = None,
    sa_relationship_args: Optional[Sequence[Any]] = None,
    sa_relationship_kwargs: Optional[Mapping[str, Any]] = None,
    disable_on_redis: bool = False,
    disable_on_sql: bool = False,
    disable_on_mongo: bool = False,
    default: Any = Undefined,
) -> Any: ...


@overload
def Relationship(
    *,
    back_populates: Optional[str] = None,
    cascade_delete: Optional[bool] = False,
    passive_deletes: Optional[Union[bool, Literal["all"]]] = False,
    link_model: Optional[Any] = None,
    sa_relationship: Optional[RelationshipProperty[Any]] = None,
    disable_on_redis: bool = False,
    disable_on_sql: bool = False,
    disable_on_mongo: bool = False,
    default: Any = Undefined,
) -> Any: ...


def Relationship(
    *,
    back_populates: Optional[str] = None,
    cascade_delete: Optional[bool] = False,
    passive_deletes: Optional[Union[bool, Literal["all"]]] = False,
    link_model: Optional[Any] = None,
    sa_relationship: Optional[RelationshipProperty[Any]] = None,
    sa_relationship_args: Optional[Sequence[Any]] = None,
    sa_relationship_kwargs: Optional[Mapping[str, Any]] = None,
    disable_on_redis: bool = False,
    disable_on_sql: bool = False,
    disable_on_mongo: bool = False,
    default: Any = Undefined,
) -> Any:
    relationship_info = RelationshipInfo(
        back_populates=back_populates,
        cascade_delete=cascade_delete,
        passive_deletes=passive_deletes,
        link_model=link_model,
        sa_relationship=sa_relationship,
        sa_relationship_args=sa_relationship_args,
        sa_relationship_kwargs=sa_relationship_kwargs,
        disable_on_redis=disable_on_redis,
        disable_on_sql=disable_on_sql,
        disable_on_mongo=disable_on_mongo,
        default=default,
    )
    return relationship_info


def get_field_definitions(
    schema: type[ModelT],
    embedded_models: dict[str, Type] | None = None,
    relationships: dict[str, Type] | None = None,
    is_for_redis: bool = False,
    is_for_mongo: bool = False,
    is_for_sql: bool = False,
) -> dict[str, tuple[Type[Any], FieldInfo]]:
    """Retrieves the field definitions from the given schema and embedded models

    Args:
        schema: the model schema class
        embedded_models: the map of embedded models as <field_name>: <type annotation>
        relationships: the map of relationships as <field_name>: <type annotation>
        is_for_redis: whether the definitions are for redis or not
        is_for_mongo: whether the definitions are for mongo or not
        is_for_sql: whether the definitions are for sql or not

    Returns:
        dict of attributes for the model in format: <name>: (<type>, <FieldInfo>)
    """
    if embedded_models is None:
        embedded_models = {}

    if relationships is None:
        relationships = {}

    fields = {}
    for field_name, field in schema.model_fields.items():  # type: str, FieldInfo
        class_field_definition = _get_class_field_definition(field)
        if is_for_redis and class_field_definition.disable_on_redis:
            continue

        if is_for_mongo and class_field_definition.disable_on_mongo:
            continue

        if is_for_sql and class_field_definition.disable_on_sql:
            continue

        field_type = field.annotation
        field_info = field

        if field_name in embedded_models:
            field_type = embedded_models[field_name]
            field_info = copy(field)
            field_info.default = class_field_definition.default

        elif field_name in relationships:
            field_type = relationships[field_name]
            # redefine the class so that SQLModel can redo its thing
            field_info = class_field_definition

        fields[field_name] = (field_type, field_info)
    return fields


def _get_class_field_definition(field: FieldInfo) -> RelationshipInfo | FieldInfo:
    """Retrieves the relationship pr field as originally defined on the class

    SQLModel seems to change Relationship definitions to field definitions
    then hiding the real Relationship in the field as the `default` value.
    Ths function retrieves the original field definition as seen on the class definition.

    Args:
        field: the field from which to extract the relationship info

    Returns:
        the relationship info if field is relationship else it returns the field
    """
    try:
        if isinstance(field.default, RelationshipInfo):
            return field.default
    except Exception:
        pass
    return field
