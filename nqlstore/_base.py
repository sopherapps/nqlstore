"""The module with the abstract classes for this package"""

import abc
from typing import Any, Iterable, TypeVar

from pydantic import BaseModel

from nqlstore.query.parsers import QueryParser
from nqlstore.query.selectors import QuerySelector

_T = TypeVar("_T", bound=BaseModel)


class BaseStore(abc.ABC):
    """Abstract class for storing data"""

    def __init__(self, uri: str, parser: QueryParser | None = None, **kwargs):
        """
        Args:
            uri: the URI to the underlying store
            parser: the query parser for parsing NQL mongodb-like queries.
            kwargs: extra key-word args to pass to the initializer
        """
        if parser is None:
            parser = QueryParser()

        self._parser = parser
        self._uri = uri

    @abc.abstractmethod
    async def register(self, models: list[type[_T]], **kwargs):
        """Registers the given models and runs any initialization steps

        Args:
            models: the list of Model's this store is to contain
            kwargs: extra key-word args to pass to the initializer
        """

    @abc.abstractmethod
    async def insert(
        self, model: type[_T], items: Iterable[_T | dict], **kwargs
    ) -> list[_T]:
        """Inserts the items to the store

        Args:
            model: the model whose instances are being inserted
            items: the items to insert into the store
            kwargs: extra key-word arguments

        Returns:
            the created items
        """

    @abc.abstractmethod
    async def find(
        self,
        model: type[_T],
        *filters: Any,
        query: QuerySelector | None = None,
        skip: int = 0,
        limit: int | None = None,
        sort: Any = None,
        **kwargs,
    ) -> list[_T]:
        """Find the items that fulfill the given filters

        Args:
            filters: the things to match against
            query: alternative mongodb-like query object to us alongside or instead of native filters
            model: the model whose instances are being queried
            skip: number of records to ignore at the top of the returned results; default is 0
            limit: maximum number of records to return; default is None.
            sort: fields to sort by; default = None
            kwargs: extra key-word args to pass to the underlying find method

        Returns:
            the matched items
        """

    @abc.abstractmethod
    async def update(
        self,
        model: type[_T],
        *filters: Any,
        query: QuerySelector | None = None,
        updates: dict | None = None,
        **kwargs,
    ) -> list[_T]:
        """Update the items that fulfill the given filters

        Args:
            filters: the things to match against
            query: alternative mongodb-like query object to us alongside or instead of native filters
            updates: the payload to update the items with
            model: the model whose instances are being updated
            kwargs: extra key-word args to pass to the underlying update method

        Returns:
            the items after updating
        """

    @abc.abstractmethod
    async def delete(
        self,
        model: type[_T],
        *filters: Any,
        query: QuerySelector | None = None,
        **kwargs,
    ) -> list[_T]:
        """Delete the items that fulfill the given filters

        Args:
            filters: the things to match against
            query: alternative mongodb-like query object to us alongside or instead of native filters
            model: the model whose instances are being deleted
            kwargs: extra key-word args to pass to the underlying delete method

        Returns:
            the deleted items
        """
