"""MongoDB implementation"""

import re
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import BaseModel
from pydantic import Field as _Field
from pydantic.main import ModelT, create_model

from nqlstore._base import BaseStore

# mongo imports
try:
    from beanie import (
        BulkWriter,
        Document,
        PydanticObjectId,
        SortDirection,
        WriteRules,
        init_beanie,
    )
    from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
except ImportError:
    Document = BaseModel
    init_beanie = lambda *a, **k: dict(**k)
    BulkWriter = Any
    SortDirection = Any
    AsyncIOMotorClient = lambda *a, **k: dict(**k)
    AsyncIOMotorClientSession = Any
    PydanticObjectId = Any

    import enum

    class WriteRules(str, enum.Enum):
        DO_NOTHING = "DO_NOTHING"
        WRITE = "WRITE"


_T = TypeVar("_T", bound=Document)
_Filter = Mapping[str, Any] | bool
_UPDATE_OP_REGEX = re.compile(r"\$\w*")


class MongoStore(BaseStore):
    """The store that persists its data in mongo db"""

    def __init__(self, uri: str, database: str, **kwargs):
        """
        Args:
            uri: the URI of the mongodb server to connect to
            database: the name of the database
            kwargs: extra key-word args to pass to AsyncIOMotorClient
        """
        super().__init__(uri, **kwargs)
        self._client = AsyncIOMotorClient(uri, **kwargs)
        self._db = self._client[database]
        self._db_name = database

    async def register(
        self,
        models: list[type[_T]],
        allow_index_dropping: bool = False,
        recreate_views: bool = False,
        multiprocessing_mode: bool = False,
        skip_indexes: bool = False,
    ):
        await init_beanie(
            self._db,
            document_models=models,
            allow_index_dropping=allow_index_dropping,
            recreate_views=recreate_views,
            multiprocessing_mode=multiprocessing_mode,
            skip_indexes=skip_indexes,
        )

    async def insert(
        self,
        model: type[_T],
        items: Iterable[_T | dict],
        session: AsyncIOMotorClientSession | None = None,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
        **pymongo_kwargs: Any,
    ) -> list[_T]:
        parsed_items = [v if isinstance(v, model) else model(**v) for v in items]
        results = await model.insert_many(
            parsed_items, session=session, link_rule=link_rule, **pymongo_kwargs
        )
        return await model.find(
            {"_id": {"$in": results.inserted_ids}}, session=session
        ).to_list()

    async def find(
        self,
        model: type[_T],
        *filters: _Filter,
        skip: int = 0,
        limit: int | None = None,
        sort: None | str | list[tuple[str, SortDirection]] = None,
        session: AsyncIOMotorClientSession | None = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        nesting_depth: int | None = None,
        nesting_depths_per_field: dict[str, int] | None = None,
        **pymongo_kwargs: Any,
    ) -> list[_T]:
        return await model.find(
            *filters,
            skip=skip,
            limit=limit,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            with_children=with_children,
            lazy_parse=lazy_parse,
            nesting_depth=nesting_depth,
            nesting_depths_per_field=nesting_depths_per_field,
            **pymongo_kwargs,
        ).to_list()

    async def update(
        self,
        model: type[_T],
        *filters: _Filter,
        updates: dict | None = None,
        session: AsyncIOMotorClientSession | None = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        nesting_depth: int | None = None,
        nesting_depths_per_field: dict[str, int] | None = None,
        bulk_writer: BulkWriter | None = None,
        upsert=False,
        **pymongo_kwargs: Any,
    ) -> list[_T]:
        if updates is None:
            updates = {}

        mongo_updates = _to_mongo_updates(updates)

        cursor = model.find(
            *filters,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            with_children=with_children,
            lazy_parse=lazy_parse,
            nesting_depth=nesting_depth,
            nesting_depths_per_field=nesting_depths_per_field,
            **pymongo_kwargs,
        )
        ids = [v.id async for v in cursor.project(_IdOnly)]
        await cursor.update(
            mongo_updates, session=session, bulk_writer=bulk_writer, upsert=upsert
        )
        return await model.find({"_id": {"$in": ids}}).to_list()

    async def delete(
        self,
        model: type[_T],
        *filters: _Filter,
        session: AsyncIOMotorClientSession | None = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        nesting_depth: int | None = None,
        nesting_depths_per_field: dict[str, int] | None = None,
        bulk_writer: BulkWriter | None = None,
        **pymongo_kwargs: Any,
    ) -> list[_T]:
        cursor = model.find(
            *filters,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            with_children=with_children,
            lazy_parse=lazy_parse,
            nesting_depth=nesting_depth,
            nesting_depths_per_field=nesting_depths_per_field,
            **pymongo_kwargs,
        )
        deleted_items = await cursor.to_list()
        await cursor.delete(session=session, bulk_writer=bulk_writer)
        return deleted_items


def MongoModel(name: str, schema: type[ModelT], /) -> type[Document]:
    """Creates a new Mongo Model for the given schema

    A new model can be defined by::

        Model = MongoModel("Model", Schema)

    Args:
        name: the name of the model
        schema: the schema from which the model is to be made

    Returns:
        a Mongo model class with the given name
    """
    return create_model(
        name,
        __doc__=schema.__doc__,
        __slots__=schema.__slots__,
        __base__=(
            Document,
            schema,
        ),
        **{
            field_name: (field.annotation, field)
            for field_name, field in schema.model_fields.items()
        },
    )


class _IdOnly(BaseModel):
    """Class used for projecting only id"""

    id: PydanticObjectId = _Field(alias="_id")


def _to_mongo_updates(updates: dict[str, Any]) -> dict[str, Any]:
    """Converts the updates dict to a MongoLike update dict if it is not one already

    Args:
        updates: the update dict to convert

    Returns:
        the mongo update with update operators
    """
    for key in updates:
        if _UPDATE_OP_REGEX.match(key):
            return updates

    return {"$set": updates}
