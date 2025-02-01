"""Module containing the QueryPredicate to be used to filter records"""

from abc import ABC, abstractmethod
from functools import reduce
from typing import Any, Iterable, Mapping, TypeVar, Union

from aredis_om import RedisModel
from aredis_om.model.model import Expression as RedisExpression
from aredis_om.model.model import Field as _RedisField
from sqlalchemy.sql._typing import _ColumnExpressionArgument
from sqlmodel import Field as _SQLField
from sqlmodel import SQLModel
from sqlmodel import and_ as _sql_and
from sqlmodel import not_ as _sql_not
from sqlmodel import or_ as _sql_or

from .selectors import OperatorSelector, QuerySelector

_SQLFilter = _ColumnExpressionArgument[bool] | bool
_MongoFilter = Mapping[str, Any] | bool
_RedisFilter = Any | RedisExpression
_T = TypeVar("_T")


class QueryPredicate(ABC):
    """This is modeled on MongoDB's query predicate for filtering records"""

    __slots__ = ("selector", "value")
    selector: str | None
    value: Any

    def __init__(self, selector: str | None, value: Any, **kwargs):
        self.selector = selector
        self.value = value

    @abstractmethod
    def to_mongo(self) -> _MongoFilter:
        """Converts this predicate to filters expected by mongo db"""
        raise NotImplementedError("MongoDB filtering not supported")

    @abstractmethod
    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        """Converts this predicate to filters expected by sqlalchemy"""
        raise NotImplementedError("SQL filtering not supported")

    @abstractmethod
    def to_redis(self) -> tuple[_RedisFilter, ...]:
        """Converts this predicate to filters expected by RedisOM"""
        raise NotImplementedError("redis filtering not supported")


class OperatorPredicate(QueryPredicate, ABC):
    """The predicate for operator-value types e.g. ``{"$eq": <value>}``"""

    __slots__ = ("parent",)
    selector: str
    value: Any
    parent: Union["FieldPredicate", "NotPredicate"]

    def __init__(
        self,
        selector: str,
        value: Any,
        parent: Union["FieldPredicate", "NotPredicate"],
        **kwargs,
    ):
        super().__init__(selector, value)
        self.parent = parent

    def to_mongo(self) -> _MongoFilter:
        return {self.selector: self.value}


class RootPredicate(QueryPredicate):
    """the root predicate that is the parent of all predicates"""

    __slots__ = (
        "__sql_model__",
        "__redis_model__",
    )
    value: list[QueryPredicate]
    selector: None
    __sql_model__: SQLModel
    __redis_model__: RedisModel

    def __init__(
        self,
        value: QuerySelector,
        __sql_model__: SQLModel | None = None,
        __redis_model__: RedisModel | None = None,
        **kwargs,
    ):
        self.__sql_model__ = __sql_model__
        self.__redis_model__ = __redis_model__
        super().__init__(selector=None, value=parse(value, parent=self))

    def to_mongo(self) -> _MongoFilter:
        return {v.selector: v.to_mongo() for v in self.value}

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        parsed_values = _flatten_list([v.to_sqlalchemy() for v in self.value])
        return tuple(parsed_values)

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        parsed_values = _flatten_list([v.to_redis() for v in self.value])
        return tuple(parsed_values)


class FieldPredicate(QueryPredicate):
    """Generic comparison or operator on a field

    Format::

        { <field>: <value> }
    """

    __slots__ = (
        "__sql_field__",
        "__redis_field__",
    )
    value: list[OperatorPredicate]
    selector: str
    __sql_field__: _SQLField
    __redis_field__: _RedisField

    def __init__(
        self,
        selector: str,
        value: OperatorSelector,
        __sql_model__: SQLModel | None = None,
        __redis_model__: RedisModel | None = None,
        **kwargs,
    ):
        if __sql_model__:
            self.__sql_field__ = getattr(__sql_model__, selector)

        if __redis_model__:
            self.__redis_field__ = getattr(__redis_model__, selector)

        super().__init__(selector=selector, value=parse(value, parent=self))

    def to_mongo(self) -> _MongoFilter:
        return {self.selector: _merge_dicts([v.to_mongo() for v in self.value])}

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        expressions = _flatten_list([v.to_sqlalchemy() for v in self.value])
        return tuple([_sql_and(*expressions)])

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        expressions = _flatten_list([v.to_redis() for v in self.value])
        return tuple([_redis_and(expressions)])


## Comparison
class EqPredicate(OperatorPredicate):
    """field is equal to value

    Format::

        { $eq: <value> }
    """

    __slots__ = ()

    def __init__(self, value: Any, parent: FieldPredicate, **kwargs):
        super().__init__(selector="$eq", value=value, parent=parent)

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        return (self.parent.__sql_field__ == self.value,)

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        return (self.parent.__redis_field__ == self.value,)


class GtPredicate(OperatorPredicate):
    """field is greater than value

    Format::

        { $gt: <value> }
    """

    __slots__ = ()

    def __init__(self, value: Any, parent: FieldPredicate, **kwargs):
        super().__init__(selector="$gt", value=value, parent=parent)

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        return (self.parent.__sql_field__ > self.value,)

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        return (self.parent.__redis_field__ > self.value,)


class GtePredicate(OperatorPredicate):
    """field is greater or equal to value

    Format::

        { $gte: <value> }
    """

    __slots__ = ()

    def __init__(self, value: Any, parent: FieldPredicate, **kwargs):
        super().__init__(selector="$gte", value=value, parent=parent)

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        return (self.parent.__sql_field__ >= self.value,)

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        return (self.parent.__redis_field__ >= self.value,)


class InPredicate(OperatorPredicate):
    """field is in list of values

    Format::

        { $in: [<value1>, <value2>, ... <valueN> ] }
    """

    __slots__ = ()

    def __init__(self, value: list[Any], parent: FieldPredicate, **kwargs):
        super().__init__(selector="$in", value=value, parent=parent)

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        return (self.parent.__sql_field__.in_(self.value),)

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        return (self.parent.__redis_field__ << self.value,)


class LtPredicate(OperatorPredicate):
    """field is less than value

    Format::

        { $lt: <value> }
    """

    __slots__ = ()

    def __init__(self, value: Any, parent: FieldPredicate, **kwargs):
        super().__init__(selector="$lt", value=value, parent=parent)

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        return (self.parent.__sql_field__ < self.value,)

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        return (self.parent.__redis_field__ < self.value,)


class LtePredicate(OperatorPredicate):
    """field is less or equal to value

    Format::

        { $lte: <value> }
    """

    __slots__ = ()

    def __init__(self, value: Any, parent: FieldPredicate, **kwargs):
        super().__init__(selector="$lte", value=value, parent=parent)

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        return (self.parent.__sql_field__ <= self.value,)

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        return (self.parent.__redis_field__ <= self.value,)


class NePredicate(OperatorPredicate):
    """field is not equal to value

    Format::

        { $ne: <value> }
    """

    __slots__ = ()

    def __init__(self, value: Any, parent: FieldPredicate, **kwargs):
        super().__init__(selector="$ne", value=value, parent=parent)

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        return (self.parent.__sql_field__ != self.value,)

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        return (self.parent.__redis_field__ != self.value,)


class NinPredicate(OperatorPredicate):
    """field is not in list of values

    Format::

        { $nin: [<value1>, <value2>, ... <valueN> ] }
    """

    __slots__ = ()

    def __init__(self, value: list[Any], parent: FieldPredicate, **kwargs):
        super().__init__(selector="$nin", value=value, parent=parent)

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        return (self.parent.__sql_field__.not_in_(self.value),)

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        return (self.parent.__redis_field__ >> self.value,)


class NotPredicate(QueryPredicate):
    """field is not <value> where value is a predicate e.g. { "$eq": <inner_value> }

    Format::

        { $not: <value>  }
    """

    __slots__ = ("__sql_field__", "__redis_field__", "parent")
    value: list[OperatorPredicate]
    selector: str
    __sql_field__: _SQLField
    __redis_field__: _RedisField
    parent: FieldPredicate

    def __init__(self, value: OperatorSelector, parent: FieldPredicate, **kwargs):
        if hasattr(parent, "__sql_field__"):
            self.__sql_field__ = parent.__sql_field__
        if hasattr(parent, "__redis_field__"):
            self.__redis_field__ = parent.__redis_field__
        self.parent = parent

        super().__init__(selector="$not", value=parse(value, parent=self))

    def to_mongo(self) -> _MongoFilter:
        positive_predicate = _merge_dicts([v.to_mongo() for v in self.value])
        return {self.parent.selector: {self.selector: positive_predicate}}

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        expressions = _flatten_list([expr.to_sqlalchemy() for expr in self.value])
        return tuple([_sql_not(_sql_and(*expressions))])

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        expressions = _flatten_list([expr.to_redis() for expr in self.value])
        return tuple([~(_redis_and(expressions))])


class MultiLogicalPredicate(QueryPredicate, ABC):
    """the base logical predicate like 'and', 'nor'"""

    __slots__ = ("parent",)
    value: list[list[QueryPredicate]]
    selector: str
    parent: QueryPredicate

    def __init__(
        self,
        value: QuerySelector,
        selector: str,
        parent: QueryPredicate,
        **kwargs,
    ):
        self.parent = parent
        super().__init__(selector=selector, value=parse(value, parent=parent))

    def to_mongo(self) -> _MongoFilter:
        predicates = [
            _merge_dicts([v.to_mongo() for v in sublist]) for sublist in self.value
        ]
        return {self.selector: predicates}


class AndPredicate(MultiLogicalPredicate):
    """all expressions are true

    Format::

        { $and: [ { <expression1> }, { <expression2> } , ... , { <expressionN> } ] }
    """

    __slots__ = ()

    def __init__(self, value: list[QuerySelector], parent: FieldPredicate, **kwargs):
        super().__init__(
            selector="$and",
            value=[parse(v, parent=parent) for v in value],
            parent=parent,
        )

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        expressions = _flatten_list(
            [expr.to_sqlalchemy() for sublist in self.value for expr in sublist]
        )
        return tuple([_sql_and(*expressions)])

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        expressions = _flatten_list(
            [expr.to_redis() for sublist in self.value for expr in sublist]
        )
        return tuple([_redis_and(expressions)])


class NorPredicate(MultiLogicalPredicate):
    """no expression is true

    Format::

        { $nor: [ { <expression1> }, { <expression2> }, ...  { <expressionN> } ] }
    """

    __slots__ = ()

    def __init__(self, value: list[QuerySelector], parent: FieldPredicate, **kwargs):
        super().__init__(
            selector="$nor",
            value=[parse(v, parent=parent) for v in value],
            parent=parent,
        )

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        expressions = _flatten_list(
            [expr.to_sqlalchemy() for sublist in self.value for expr in sublist]
        )
        return tuple([_sql_not(_sql_or(*expressions))])

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        expressions = _flatten_list(
            [expr.to_redis() for sublist in self.value for expr in sublist]
        )
        return tuple([_redis_and([~expr for expr in expressions])])


class OrPredicate(MultiLogicalPredicate):
    """any expression is true

    Format::

        { $or: [ { <expression1> }, { <expression2> }, ...  { <expressionN> } ] }
    """

    __slots__ = ()

    def __init__(self, value: list[QuerySelector], parent: FieldPredicate, **kwargs):
        super().__init__(
            selector="$or",
            value=[parse(v, parent=parent) for v in value],
            parent=parent,
        )

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        expressions = _flatten_list(
            [expr.to_sqlalchemy() for sublist in self.value for expr in sublist]
        )
        return tuple([_sql_or(*expressions)])

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        expressions = _flatten_list(
            [expr.to_redis() for sublist in self.value for expr in sublist]
        )
        return tuple([_redis_or(expressions)])


# TODO Add more starting at https://www.mongodb.com/docs/manual/reference/operator/query/#element

_QUERY_PREDICATE_MAP: dict[str, type[QueryPredicate]] = {
    "$eq": EqPredicate,
    "$gt": GtPredicate,
    "$gte": GtePredicate,
    "$in": InPredicate,
    "$lt": LtPredicate,
    "$lte": LtePredicate,
    "$ne": NePredicate,
    "$nin": NinPredicate,
    "$not": NotPredicate,
    "$and": AndPredicate,
    "$nor": NorPredicate,
    "$or": OrPredicate,
}


def parse(
    selector: QuerySelector | OperatorSelector, parent: QueryPredicate | None = None
) -> list[QueryPredicate]:
    """Converts the query selector to a stronger typed query predicates

    Args:
        selector: the mongo-like query filter dict
        parent: the parent of the given selector

    Returns:
        the strongly typed query predicates
    """
    # extra optional args to pass to initializer
    kwargs = dict(
        __sql_model__=getattr(parent, "__sql_model__", None),
        __redis_model__=getattr(parent, "__redis_model__", None),
        parent=parent,
    )

    return [
        _QUERY_PREDICATE_MAP.get(k, FieldPredicate)(selector=k, value=v, **kwargs)
        for k, v in selector.items()
    ]


def _flatten_list(values: Iterable[Iterable[_T]]) -> list[_T]:
    """Converts a nested iterable into a non-nested iterable

    Args:
        values: the nested iterable

    Returns:
        the flattened iterable
    """
    return [item for sublist in values for item in sublist]


def _merge_dicts(values: Iterable[Mapping]) -> dict:
    """Converts an iterable of dicts into a single dict

    Args:
        values: the list of dicts

    Returns:
        the merged dict
    """
    return {k: v for item in values for k, v in item.items()}


def _redis_and(__filters: list[_RedisFilter]) -> _RedisFilter:
    """Merges multiple redis filters into one with 'AND' logical operator

    Args:
        __filters: the redis filters

    Returns:
        the merged AND filter
    """
    return reduce(lambda prev, curr: prev & curr, __filters)


def _redis_or(__filters: list[_RedisFilter]) -> _RedisFilter:
    """Merges multiple redis filters into one with 'OR' logical operator

    Args:
        __filters: the redis filters

    Returns:
        the merged OR filter
    """
    return reduce(lambda prev, curr: prev | curr, __filters)
