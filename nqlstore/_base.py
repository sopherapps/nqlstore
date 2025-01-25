"""The module with the abstract classes for this package"""

import abc
from typing import Any, Iterable, TypeVar

from pydantic import BaseModel

_T = TypeVar("_T", bound=BaseModel)


class BaseStore(abc.ABC):
    """Abstract class for storing data"""

    def __init__(self, uri: str, **kwargs):
        """
        Args:
            uri: the URI to the underlying store
        """
        self._uri = uri

    @abc.abstractmethod
    def register(self, models: Iterable[type[_T]]):
        """Registers the given models and runs any initialization steps

        Args:
            models: the list of Model's this store is to contain
        """

    @abc.abstractmethod
    def insert(
        self, model: type[_T], items: Iterable[_T | dict], **kwargs
    ) -> Iterable[_T]:
        """Inserts the items to the store

        Args:
            model: the model whose instances are being inserted
            items: the items to insert into the store
            kwargs: extra key-word arguments

        Returns:
            the created items
        """

    @abc.abstractmethod
    def find(
        self, model: type[_T], *filters: Any, skip: int = 0, limit: int | None = None
    ) -> Iterable[_T]:
        """Find the items that fulfill the given filters

        Args:
            filters: the things to match against
            model: the model whose instances are being queried
            skip: number of records to ignore at the top of the returned results; default is 0
            limit: maximum number of records to return; default is None.

        Returns:
            the matched items
        """

    @abc.abstractmethod
    def update(self, model: type[_T], *filters: Any, updates: dict) -> Iterable[_T]:
        """Update the items that fulfill the given filters

        Args:
            filters: the things to match against
            updates: the payload to update the items with
            model: the model whose instances are being updated

        Returns:
            the items after updating
        """

    @abc.abstractmethod
    def delete(self, model: type[_T], *filters: Any) -> Iterable[_T]:
        """Delete the items that fulfill the given filters

        Args:
            filters: the things to match against
            model: the model whose instances are being deleted

        Returns:
            the deleted items
        """
