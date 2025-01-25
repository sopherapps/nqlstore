"""Redis implementation"""

from typing import Any, AsyncIterable, Callable, Iterable, TypeVar

from aredis_om import *
from aredis_om.model.model import Expression, verify_pipeline_response
from redis.client import Pipeline

from nqlstore._base import BaseStore

_T = TypeVar("_T", bound=RedisModel)


class RedisStore(BaseStore):
    """The store with data persisted in redis"""

    def __init__(self, uri: str, **kwargs):
        super().__init__(uri, **kwargs)
        self._db = get_redis_connection(url=uri, **kwargs)

    async def register(self, models: list[type[_T]], **kwargs):
        # set the redis instances of all passed models to the current redis instance
        for model in models:
            model.Meta.database = self._db

    async def insert(
        self,
        model: type[_T],
        items: Iterable[_T | dict],
        pipeline: Pipeline | None = None,
        pipeline_verifier: Callable[..., Any] = verify_pipeline_response,
        **kwargs,
    ) -> AsyncIterable[_T]:
        parsed_items = [v if isinstance(v, model) else model(**v) for v in items]
        results = await model.add(
            parsed_items, pipeline=pipeline, pipeline_verifier=pipeline_verifier
        )

        for result in results:
            yield result

    async def find(
        self,
        model: type[_T],
        *filters: Any | Expression,
        skip: int = 0,
        limit: int | None = None,
        sort: tuple[str] | None = None,
        knn: KNNExpression | None = None,
        **kwargs,
    ) -> AsyncIterable[_T]:
        query = model.find(*filters, knn=knn)
        return await query.copy(
            offset=skip, sort_fields=sort, limit=limit, **kwargs
        ).all()

    async def update(
        self,
        model: type[_T],
        *filters: Any | Expression,
        updates: dict,
        knn: KNNExpression | None = None,
        **kwargs,
    ) -> AsyncIterable[_T]:
        query = model.find(*filters, knn=knn)
        matched_items = await query.copy(**kwargs).all()
        updated_pks = []
        for item in matched_items:
            await item.update(**updates)
            updated_pks.append(item.pk)

        return await model.find(model.pk << updated_pks).all()

    async def delete(
        self,
        model: type[_T],
        *filters: Any | Expression,
        knn: KNNExpression | None = None,
        pipeline: Pipeline | None = None,
        **kwargs,
    ) -> AsyncIterable[_T]:
        query = model.find(*filters, knn=knn)
        matched_items = await query.copy(**kwargs).all()
        await model.delete_many(matched_items, pipeline=pipeline)

        return matched_items
