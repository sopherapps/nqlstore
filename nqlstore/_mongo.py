"""MongoDB implementation"""

import re
import sys
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import BaseModel
from pydantic import Field as _Field
from pydantic.main import ModelT, create_model

from ._base import BaseStore
from ._compat import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    Document,
    PydanticObjectId,
    SortDirection,
    init_beanie,
)
from ._field import get_field_definitions

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
        """Registers the given models and runs any initialization steps

        Args:
            models: the list of Model's this store is to contain
            allow_index_dropping: if index dropping is allowed.
                Default False
            recreate_views: if views should be recreated. Default False
            multiprocessing_mode: bool - if multiprocessing mode is on
                it will patch the motor client to use process's event loop. Default False
            skip_indexes: if you want to skip working with the indexes.
                Default False
        """
        document_models = [
            cls for cls in models if not issubclass(cls, _EmbeddedMongoModel)
        ]
        await init_beanie(
            self._db,
            document_models=document_models,
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
        **pymongo_kwargs: Any,
    ) -> list[_T]:
        # parse them so that any default values from the model definition are added,
        # and proper validation is done
        parsed_items = [
            v if isinstance(v, model) else model.model_validate(v) for v in items
        ]
        items_as_dicts = [v.model_dump() for v in parsed_items]
        collection = self._get_collection(model)

        insert_result = await collection.insert_many(
            items_as_dicts, session=session, **pymongo_kwargs
        )
        raw_results = await collection.find(
            {"_id": {"$in": insert_result.inserted_ids}}, session=session
        ).to_list()
        return [model.model_validate(v) for v in raw_results]

    async def find(
        self,
        model: type[_T],
        query: _Filter | None = None,
        skip: int = 0,
        limit: int = 0,
        sort: None | str | list[tuple[str, SortDirection]] = None,
        session: AsyncIOMotorClientSession | None = None,
        **pymongo_kwargs: Any,
    ) -> list[_T]:
        if query is None:
            query = {}

        collection = self._get_collection(model)

        raw_results = await collection.find(
            query,
            skip=skip,
            limit=limit,
            session=session,
            sort=sort,
            **pymongo_kwargs,
        ).to_list()

        return [model.model_validate(v) for v in raw_results]

    async def update(
        self,
        model: type[_T],
        query: _Filter | None = None,
        updates: dict | None = None,
        session: AsyncIOMotorClientSession | None = None,
        upsert=False,
        **pymongo_kwargs: Any,
    ) -> list[_T]:
        if updates is None:
            updates = {}

        if query is None:
            query = {}

        mongo_updates = _to_mongo_updates(updates)

        collection = self._get_collection(model)
        query_cursor = collection.find(
            query, projection={"_id": True}, session=session, **pymongo_kwargs
        )
        ids = [v["_id"] async for v in query_cursor]

        await collection.update_many(
            query,
            update=mongo_updates,
            session=session,
            upsert=upsert,
            **pymongo_kwargs,
        )
        raw_results = collection.find(
            {"_id": {"$in": ids}}, session=session, **pymongo_kwargs
        )
        return [model.model_validate(v) async for v in raw_results]

    async def delete(
        self,
        model: type[_T],
        query: _Filter | None = None,
        session: AsyncIOMotorClientSession | None = None,
        **pymongo_kwargs: Any,
    ) -> list[_T]:
        if query is None:
            query = {}

        collection = self._get_collection(model)
        query_cursor = collection.find(query, session=session, **pymongo_kwargs)
        deleted_items = [model.model_validate(v) async for v in query_cursor]
        await collection.delete_many(query, session=session, **pymongo_kwargs)
        return deleted_items

    def _get_collection(self, model: type[_T]) -> AsyncIOMotorCollection:
        """Gets the collection for the given model

        Args:
            model: the model class whose collection is to be obtained

        Returns:
            the AsyncIOMotorCollection for the given model
        """
        collection_name = model.get_collection_name()
        return self._db[collection_name]


class _EmbeddedMongoModel(BaseModel):
    """An embedded model for mongo database

    It is never created in the database as a collection
    """

    pass


def MongoModel(
    name: str,
    schema: type[ModelT],
    /,
    embedded_models: dict[
        str,
        type[_EmbeddedMongoModel]
        | type[list[_EmbeddedMongoModel]]
        | type[ModelT]
        | type[list[ModelT]],
    ] = None,
) -> type[Document] | type[ModelT]:
    """Creates a new Mongo Model for the given schema

    A new model can be defined by::

        Model = MongoModel("Model", Schema, embedded_models = {"address": Address})

    Args:
        name: the name of the model
        schema: the schema from which the model is to be made
        embedded_models: a dict of embedded models of <field name>: annotation

    Returns:
        a Mongo model class with the given name
    """
    fields = get_field_definitions(
        schema, embedded_models=embedded_models, is_for_mongo=True
    )

    model = create_model(
        name,
        # module of the calling function
        __module__=sys._getframe(1).f_globals["__name__"],
        __doc__=schema.__doc__,
        __base__=(Document,),
        **fields,
    )

    _copy_settings(dst=model, src=schema)
    return model


def EmbeddedMongoModel(
    name: str,
    schema: type[ModelT],
    /,
    embedded_models: dict[
        str,
        type[_EmbeddedMongoModel]
        | type[list[_EmbeddedMongoModel]]
        | type[ModelT]
        | type[list[ModelT]],
    ] = None,
) -> type[_EmbeddedMongoModel] | type[ModelT]:
    """Creates a new embedded Mongo Model for the given schema

    A new model can be defined by::

        Model = EmbeddedMongoModel("Model", Schema)

    Args:
        name: the name of the model
        schema: the schema from which the model is to be made
        embedded_models: a dict of embedded models of <field name>: annotation

    Returns:
        an embedded Mongo model class with the given name
    """
    fields = get_field_definitions(
        schema, embedded_models=embedded_models, is_for_mongo=True
    )

    return create_model(
        name,
        # module of the calling function
        __module__=sys._getframe(1).f_globals["__name__"],
        __doc__=schema.__doc__,
        __base__=(_EmbeddedMongoModel,),
        **fields,
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


def _copy_settings(dst: type[Document], src: type[ModelT]):
    """Copies settings from source to destination

    Args:
        dst: the model to receive the settings
        src: the model or schema that contains the settings
    """
    try:
        settings_cls = getattr(src, "Settings")
        setattr(dst, "Settings", settings_cls)
    except AttributeError:
        pass
