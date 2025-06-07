"""Some random utilities for the app"""

import copy
import sys
from datetime import datetime
from typing import Any, Literal, Optional, TypeVar, get_args

from pydantic import BaseModel, create_model
from pydantic.main import IncEx

from nqlstore._field import FieldInfo

_T = TypeVar("_T", bound=BaseModel)


def current_timestamp() -> str:
    """Gets the current timestamp as an timezone naive ISO format string

    Returns:
        string of the current datetime
    """
    return datetime.now().isoformat()


def Partial(name: str, model: type[_T]) -> type[_T]:
    """Creates a partial schema from another schema, with all fields optional

    Args:
        name: the name of the model
        model: the original model

    Returns:
        A new model with all its fields optional
    """
    fields = {
        k: (_make_optional(v.annotation), None)
        for k, v in model.model_fields.items()  # type: str, FieldInfo
    }

    return create_model(
        name,
        # module of the calling function
        __module__=sys._getframe(1).f_globals["__name__"],
        __doc__=model.__doc__,
        __base__=(model,),
        **fields,
    )


def _make_optional(type_: type) -> type:
    """Makes a type optional if not optional

    Args:
        type_: the type to make optional

    Returns:
        the optional type
    """
    if type(None) in get_args(type_):
        return type_
    return type_ | None
