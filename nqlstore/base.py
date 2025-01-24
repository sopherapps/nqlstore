import abc
from dataclasses import dataclass
from typing import List, Any, Dict, Type, TypeVar, Optional

T = TypeVar("T")


@dataclass(frozen=True)
class Filter:
    """The filter object to be passed to store functions when needed"""
    greater_than: Optional[Dict] = None
    greater_or_equal: Optional[Dict] = None
    less_than: Optional[Dict] = None
    less_or_equal: Optional[Dict] = None
    equal: Optional[Dict] = None
    or_: Optional[List["Filter"]] = None
    and_: Optional[List["Filter"]] = None


class Store(abc.ABC):
    """Abstract class for storing data"""

    @abc.abstractmethod
    def insert(self, items: List[T], model: Type[T]=None) -> List[T]:
        """Inserts the items to the store

        Args:
            items: the items to insert into the store
            model: the model whose instances are being inserted

        Returns:
            the created items
        """

    @abc.abstractmethod
    def find(self, filters: Filter, model: Type[T]=None) -> List[T]:
        """Find the items that fulfill the given filters

        Args:
            filters: the things to match against
            model: the model whose instances are being queried

        Returns:
            the matched items
        """

    @abc.abstractmethod
    def update(self, filters: Filter, update: Dict[str, Any], model: Type[T]=None) -> List[T]:
        """Update the items that fulfill the given filters

        Args:
            filters: the things to match against
            update: the payload to update the items with
            model: the model whose instances are being updated

        Returns:
            the items after updating
        """

    @abc.abstractmethod
    def delete(self, filters: Filter, model: Type[T]=None) -> List[T]:
        """Delete the items that fulfill the given filters

        Args:
            filters: the things to match against
            model: the model whose instances are being deleted

        Returns:
            the deleted items
        """