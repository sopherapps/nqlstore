"""Module containing all possible query selectors

See: https://www.mongodb.com/docs/manual/reference/operator/query/#std-label-query-selectors
"""

from typing import Any, TypedDict

# Query Selectors

## Comparison
_EqSelector = TypedDict("_EqSelector", {"$eq": Any})
"""field is equal to value: ``{ $eq: <value> }``"""

_GtSelector = TypedDict("_GtSelector", {"$gt": Any})
"""field is greater than value: ``{ $gt: value }``"""

_GteSelector = TypedDict("_GteSelector", {"$gte": Any})
"""field is greater or equal to value: ``{ $gte: value }``"""

_InSelector = TypedDict("_InSelector", {"$in": list[Any]})
"""field is in list: ``{ $in: [<value1>, <value2>, ... <valueN> ] }``"""

_LtSelector = TypedDict("_LtSelector", {"$lt": Any})
"""field is less than value: ``{ $lt: value }``"""

_LteSelector = TypedDict("_LteSelector", {"$lte": Any})
"""field is less or equal to value: ``{ $lte: value }``"""

_NeSelector = TypedDict("_NeSelector", {"$ne": Any})
"""field is not equal to value: ``{ $ne: value }``"""

_NinSelector = TypedDict("_NinSelector", {"$nin": list[Any]})
"""field is not in list: ``{ $nin: [ <value1>, <value2> ... <valueN> ] }``"""


_RegexSelector = TypedDict(
    "_RegexSelector", {"$regex": str, "$options": str}, total=False
)
"""field matches given regular expression: ``{ "$regex": "pattern", "$options": "<options>" }``

``$options`` is optional
"""

## Element
_ExistsSelector = TypedDict("_ExistsSelector", {"$exists": bool})
"""field exists: ``{ $exists: <boolean> }``"""

_TypeSelector = TypedDict("_TypeSelector", {"$type": Any})
"""field is given BSON type: ``{ $type: <value1> }``"""

## Logical
OperatorSelector = (
    _EqSelector
    | _GtSelector
    | _GteSelector
    | _InSelector
    | _LtSelector
    | _LteSelector
    | _NeSelector
    | _NinSelector
    | _ExistsSelector
    | _TypeSelector
)
FieldSelector = dict[str, OperatorSelector]
"""a comparison on a given field: ``{ field: { <operator>: <operand>}}``"""

_AndSelector = TypedDict("_AndSelector", {"$and": list[FieldSelector]})
"""all expressions are true: ``{ $and: [ { <expression1> }, { <expression2> } , ... , { <expressionN> } ] }``"""

_NotSelector = TypedDict("_NotSelector", {"$not": OperatorSelector})
"""not true: ``{ field: { $not: { <operator-expression> } } }``"""

_NorSelector = TypedDict("_NorSelector", {"$nor": list[FieldSelector]})
"""no expression is true: ``{ $nor: [ { <expression1> }, { <expression2> }, ...  { <expressionN> } ] }``"""

_OrSelector = TypedDict("_OrSelector", {"$or": list[FieldSelector]})
"""any expression is true: ``{ $or: [ { <expression1> }, { <expression2> }, ... , { <expressionN> } ] }``"""

# FIXME: Add more starting at https://www.mongodb.com/docs/manual/reference/operator/query/#element

QuerySelector = FieldSelector | _AndSelector | _NotSelector | _NorSelector | _OrSelector
"""query predicate for filtering records"""
