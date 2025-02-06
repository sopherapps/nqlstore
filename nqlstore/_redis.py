"""Redis implementation"""

from typing import Any, Callable, Iterable, TypeVar

from aredis_om import EmbeddedJsonModel as _EmbeddedJsonModel
from aredis_om import HashModel as _HashModel
from aredis_om import JsonModel as _JsonModel
from aredis_om import KNNExpression, Migrator
from aredis_om import RedisModel as _RedisModel
from aredis_om import get_redis_connection
from aredis_om.model.model import Expression, verify_pipeline_response
from pydantic.main import ModelT, create_model
from redis.client import Pipeline

from nqlstore._base import BaseStore
from nqlstore.query.parsers import QueryParser
from nqlstore.query.selectors import QuerySelector

_T = TypeVar("_T", bound=_RedisModel)


class RedisStore(BaseStore):
    """The store with data persisted in redis"""

    def __init__(self, uri: str, parser: QueryParser | None = None, **kwargs):
        super().__init__(uri, parser=parser, **kwargs)
        self._db = get_redis_connection(url=uri, **kwargs)

    async def register(self, models: list[type[_T]], **kwargs):
        # set the redis instances of all passed models to the current redis instance
        for model in models:
            model.Meta.database = self._db
        await Migrator().run()

    async def insert(
        self,
        model: type[_T],
        items: Iterable[_T | dict],
        pipeline: Pipeline | None = None,
        pipeline_verifier: Callable[..., Any] = verify_pipeline_response,
        **kwargs,
    ) -> list[_T]:
        parsed_items = [v if isinstance(v, model) else model(**v) for v in items]
        results = await model.add(
            parsed_items, pipeline=pipeline, pipeline_verifier=pipeline_verifier
        )
        return list(results)

    async def find(
        self,
        model: type[_T],
        *filters: Any | Expression,
        query: QuerySelector | None = None,
        skip: int = 0,
        limit: int | None = None,
        sort: tuple[str] | None = None,
        knn: KNNExpression | None = None,
        **kwargs,
    ) -> list[_T]:
        nql_filters = ()
        if query:
            nql_filters = self._parser.to_redis(model, query=query)

        query = model.find(*filters, *nql_filters, knn=knn)

        kwargs["offset"] = skip
        kwargs["limit"] = limit
        if sort:
            kwargs["sort_fields"] = sort

        return await query.copy(**kwargs).all()

    async def update(
        self,
        model: type[_T],
        *filters: Any | Expression,
        query: QuerySelector | None = None,
        updates: dict | None = None,
        knn: KNNExpression | None = None,
        **kwargs,
    ) -> list[_T]:
        if updates is None:
            updates = {}

        nql_filters = ()
        if query:
            nql_filters = self._parser.to_redis(model, query=query)

        query = model.find(*filters, *nql_filters, knn=knn)
        matched_items = await query.copy(**kwargs).all()
        updated_pks = []
        for item in matched_items:
            await item.update(**updates)
            updated_pks.append(item.pk)

        return await model.find((model.pk << updated_pks)).all()

    async def delete(
        self,
        model: type[_T],
        *filters: Any | Expression,
        query: QuerySelector | None = None,
        knn: KNNExpression | None = None,
        pipeline: Pipeline | None = None,
        **kwargs,
    ) -> list[_T]:
        nql_filters = ()
        if query:
            nql_filters = self._parser.to_redis(model, query=query)

        query = model.find(*filters, *nql_filters, knn=knn)
        matched_items = await query.copy(**kwargs).all()
        await model.delete_many(matched_items, pipeline=pipeline)
        return matched_items


def HashModel(name: str, schema: type[ModelT], /) -> type[ModelT]:
    """Creates a new HashModel for the given schema for redis

    A new model can be defined by::

        Model = HashModel("Model", Schema)

    Args:
        name: the name of the model
        schema: the schema from which the model is to be made

    Returns:
        a HashModel model class with the given name
    """
    # FIXME: Handle scenario where a pk is defined
    return create_model(
        name,
        __doc__=schema.__doc__,
        __slots__=schema.__slots__,
        __base__=(
            _HashModel,
            schema,
        ),
        **{
            field_name: (field.annotation, field)
            for field_name, field in schema.model_fields.items()
        },
    )


def JsonModel(name: str, schema: type[ModelT], /) -> type[ModelT]:
    """Creates a new JsonModel for the given schema for redis

    A new model can be defined by::

        Model = JsonModel("Model", Schema)

    Args:
        name: the name of the model
        schema: the schema from which the model is to be made

    Returns:
        a JsonModel model class with the given name
    """
    # FIXME: Handle scenario where a pk is defined
    return create_model(
        name,
        __doc__=schema.__doc__,
        __slots__=schema.__slots__,
        __base__=(
            _JsonModel,
            schema,
        ),
        **{
            field_name: (field.annotation, field)
            for field_name, field in schema.model_fields.items()
        },
    )


def EmbeddedJsonModel(name: str, schema: type[ModelT], /) -> type[ModelT]:
    """Creates a new EmbeddedJsonModel for the given schema for redis

    A new model can be defined by::

        Model = EmbeddedJsonModel("Model", Schema)

    Args:
        name: the name of the model
        schema: the schema from which the model is to be made

    Returns:
        a EmbeddedJsonModel model class with the given name
    """
    # FIXME: Handle scenario where a pk is defined
    return create_model(
        name,
        __doc__=schema.__doc__,
        __slots__=schema.__slots__,
        __base__=(
            _EmbeddedJsonModel,
            schema,
        ),
        **{
            field_name: (field.annotation, field)
            for field_name, field in schema.model_fields.items()
        },
    )
