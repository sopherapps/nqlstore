"""Module containing the QueryPredicate to be used to filter records"""

import logging
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

    __slots__ = ("selector", "value", "parser")
    selector: str | None
    value: Any
    parser: "Parser"

    def __init__(self, selector: str | None, value: Any, parser: "Parser", **kwargs):
        self.selector = selector
        self.value = value
        self.parser = parser

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
        super().__init__(selector, value, parser=parent.parser)
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
        parser: Union["Parser", None] = None,
        __sql_model__: SQLModel | None = None,
        __redis_model__: RedisModel | None = None,
        **kwargs,
    ):
        self.__sql_model__ = __sql_model__
        self.__redis_model__ = __redis_model__
        if parser is None:
            parser = Parser()
        super().__init__(selector=None, value=None, parser=parser)
        self.value = self.parser.parse(value, parent=self)

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
        "parent",
    )
    value: list[OperatorPredicate]
    selector: str
    __sql_field__: _SQLField
    __redis_field__: _RedisField
    parent: QueryPredicate

    def __init__(
        self,
        selector: str,
        value: OperatorSelector,
        parent: QueryPredicate,
        __sql_model__: SQLModel | None = None,
        __redis_model__: RedisModel | None = None,
        **kwargs,
    ):
        if __sql_model__:
            self.__sql_field__ = getattr(__sql_model__, selector)

        if __redis_model__:
            self.__redis_field__ = getattr(__redis_model__, selector)

        super().__init__(selector=selector, value=None, parser=parent.parser)
        self.value = self.parser.parse(value, parent=self)

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
        super().__init__(selector="$eq", value=value, parent=parent, **kwargs)

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
        super().__init__(selector="$gt", value=value, parent=parent, **kwargs)

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
        super().__init__(selector="$gte", value=value, parent=parent, **kwargs)

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
        super().__init__(selector="$in", value=value, parent=parent, **kwargs)

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
        super().__init__(selector="$lt", value=value, parent=parent, **kwargs)

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
        super().__init__(selector="$lte", value=value, parent=parent, **kwargs)

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
        super().__init__(selector="$ne", value=value, parent=parent, **kwargs)

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
        super().__init__(selector="$nin", value=value, parent=parent, **kwargs)

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

        super().__init__(selector="$not", value=None, **kwargs)
        self.value = self.parser.parse(value, parent=self)

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
        value: list[QuerySelector],
        selector: str,
        parent: QueryPredicate,
        **kwargs,
    ):
        self.parent = parent
        super().__init__(selector=selector, value=None, parser=parent.parser, **kwargs)
        self.value = [self.parser.parse(v, parent=parent) for v in value]

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
        super().__init__(selector="$and", value=value, parent=parent, **kwargs)

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
            value=value,
            parent=parent,
            **kwargs,
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
            value=value,
            parent=parent,
            **kwargs,
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


class MongoOnlyPredicate(QueryPredicate):
    """Generic predicate that handles any queries that are relevant to Mongo only"""

    __slots__ = (
        "__sql_model__",
        "__redis_model__",
        "__sql_field__",
        "__redis_field__",
        "parent",
    )
    parent: QueryPredicate

    def __init__(self, value: Any, selector: str, **kwargs):
        parent: QueryPredicate = kwargs.get("parent")
        self.parent = parent
        super().__init__(selector=selector, value=value, parser=parent.parser, **kwargs)
        for k, v in kwargs.items():
            try:
                setattr(self, k, v)
            except AttributeError:
                pass

    def to_mongo(self) -> _MongoFilter:
        return {self.selector: self.value}

    def to_sqlalchemy(self) -> tuple[_SQLFilter, ...]:
        logging.warning(f"{self.selector} might not work as expected in SQL databases")
        return ()

    def to_redis(self) -> tuple[_RedisFilter, ...]:
        logging.warning(f"{self.selector} might not work as expected in redis")
        return ()


class Parser(dict):
    """The registry of parsers of each section of the query selector

    This can be overridden to provide custom parsers for specific query selectors
    """

    _parsers: dict[str, type[QueryPredicate]] = {
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
        "$exists": MongoOnlyPredicate,
        "$type": MongoOnlyPredicate,
        "$expr": MongoOnlyPredicate,
        "$jsonSchema": MongoOnlyPredicate,
        "$mod": MongoOnlyPredicate,  # TODO: implement this for SQL and redis
        "$regex": MongoOnlyPredicate,  # TODO: implement this for SQL and redis, too complex
        "$text": MongoOnlyPredicate,  # TODO: implement this for SQL and redis, too complex
        "$where": MongoOnlyPredicate,
        "$geoIntersects": MongoOnlyPredicate,
        "$geoWithin": MongoOnlyPredicate,
        "$near": MongoOnlyPredicate,
        "$nearSphere": MongoOnlyPredicate,
        "$all": MongoOnlyPredicate,
        "$elemMatch": MongoOnlyPredicate,
        "$size": MongoOnlyPredicate,  # TODO: implement this for SQL and redis
        "$bitsAllClear": MongoOnlyPredicate,
        "$bitsAllSet": MongoOnlyPredicate,
        "$bitsAnyClear": MongoOnlyPredicate,
        "$bitsAnySet": MongoOnlyPredicate,
        "$": MongoOnlyPredicate,
        "$meta": MongoOnlyPredicate,
        "$slice": MongoOnlyPredicate,
        "$rand": MongoOnlyPredicate,
        "$natural": MongoOnlyPredicate,
        "$options": MongoOnlyPredicate,
    }

    def __init__(self, overrides: dict[str, type[QueryPredicate]] | None = None):
        """Initialize the parser registry class with any parsers overridden or new ones added

        The defaults parsers are as follows::

            {
                "$eq": :class:`~EqPredicate`,
                "$gt": :class:`~GtPredicate`,
                "$gte": :class:`~GtePredicate`,
                "$in": :class:`~InPredicate`,
                "$lt": :class:`~LtPredicate`,
                "$lte": :class:`~LtePredicate`,
                "$ne": :class:`~NePredicate`,
                "$nin": :class:`~NinPredicate`,
                "$not": :class:`~NotPredicate`,
                "$and": :class:`~AndPredicate`,
                "$nor": :class:`~NorPredicate`,
                "$or": :class:`~OrPredicate`,
                "$exists": :class:`~MongoOnlyPredicate`,
                "$type": :class:`~MongoOnlyPredicate`,
                "$expr": :class:`~MongoOnlyPredicate`,
                "$jsonSchema": :class:`~MongoOnlyPredicate`,
                "$mod": :class:`~MongoOnlyPredicate`,
                "$regex": :class:`~MongoOnlyPredicate`,
                "$text": :class:`~MongoOnlyPredicate`,
                "$where": :class:`~MongoOnlyPredicate`,
                "$geoIntersects": :class:`~MongoOnlyPredicate`,
                "$geoWithin": :class:`~MongoOnlyPredicate`,
                "$near": :class:`~MongoOnlyPredicate`,
                "$nearSphere": :class:`~MongoOnlyPredicate`,
                "$all": :class:`~MongoOnlyPredicate`,
                "$elemMatch": :class:`~MongoOnlyPredicate`,
                "$size": :class:`~MongoOnlyPredicate`,
                "$bitsAllClear": :class:`~MongoOnlyPredicate`,
                "$bitsAllSet": :class:`~MongoOnlyPredicate`,
                "$bitsAnyClear": :class:`~MongoOnlyPredicate`,
                "$bitsAnySet": :class:`~MongoOnlyPredicate`,
                "$": :class:`~MongoOnlyPredicate`,
                "$meta": :class:`~MongoOnlyPredicate`,
                "$slice": :class:`~MongoOnlyPredicate`,
                "$rand": :class:`~MongoOnlyPredicate`,
                "$natural": :class:`~MongoOnlyPredicate`,
                "$options": :class:`~MongoOnlyPredicate`,
            }

        Args:
            overrides: a dictionary with similar structure as _parsers above to override or add
                new selector parsers
        """
        if overrides:
            self._parsers = {**self._parsers, **overrides}

        super().__init__()

    def parse(
        self,
        selector: QuerySelector | OperatorSelector,
        parent: QueryPredicate,
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
            self._get_predicate_cls(k)(selector=k, value=v, **kwargs)
            for k, v in selector.items()
        ]

    def _get_predicate_cls(self, selector: str) -> type[QueryPredicate]:
        """Gets the appropriate QueryPredicate class basing on selector

        Args:
            selector: the selector string like ``$and``, ``$not`` etc

        Returns:
            the right Predicate class
        """
        try:
            return self._parsers[selector]
        except KeyError:
            if selector.startswith("$"):
                raise NotImplementedError(
                    f"selector {selector} not supported yet. Try the native interface."
                )
            return FieldPredicate


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
