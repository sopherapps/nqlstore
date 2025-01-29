import pytest

from nqlstore.sql import SQLStore


@pytest.fixture
def sql_store():
    """The sql store stored in memory"""
    store = SQLStore(uri="sqlite+aiosqlite:///:memory:")
    yield store
