"""Tests for the query package"""

import pytest

from tests.conftest import RedisLibrary, SqlLibrary
from tests.utils import is_lib_installed, to_sql_text


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_eq_redis(redis_qparser):
    """$eq checks equality in redis"""
    query = {"name": {"$eq": "Hoima, Uganda"}}
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name == "Hoima, Uganda"),)


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_eq_sql(sql_qparser):
    """$eq checks equality in sql"""
    query = {"name": {"$eq": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name == "Hoima, Uganda"),))
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_gt_redis(redis_qparser):
    """$gt checks greater than in redis"""
    query = {"name": {"$gt": "Hoima, Uganda"}}
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name > "Hoima, Uganda"),)


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_gt_sql(sql_qparser):
    """$gt checks greater than in sql"""
    query = {"name": {"$gt": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name > "Hoima, Uganda"),))
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_gte_redis(redis_qparser):
    """$gte checks greater or equal in redis"""
    query = {"name": {"$gte": "Hoima, Uganda"}}
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name >= "Hoima, Uganda"),)


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_gte_sql(sql_qparser):
    """$gte checks greater or equal in sql"""
    query = {"name": {"$gte": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name >= "Hoima, Uganda"),))
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_in_redis(redis_qparser):
    """$in checks value in list in redis"""
    query = {"name": {"$in": ["Hoima, Uganda"]}}
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name << ["Hoima, Uganda"]),)


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_in_sql(sql_qparser):
    """$in checks vlue in list than in sql"""
    query = {"name": {"$in": ["Hoima, Uganda"]}}
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name.in_(["Hoima, Uganda"])),))
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_lt_redis(redis_qparser):
    """$lt checks less than in redis"""
    query = {"name": {"$lt": "Hoima, Uganda"}}
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name < "Hoima, Uganda"),)


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_lt_sql(sql_qparser):
    """$lt checks less than in sql"""
    query = {"name": {"$lt": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name < "Hoima, Uganda"),))
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_lte_redis(redis_qparser):
    """$lt checks less or equal in redis"""
    query = {"name": {"$lte": "Hoima, Uganda"}}
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name <= "Hoima, Uganda"),)


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_lte_sql(sql_qparser):
    """$lt checks less or equal in sql"""
    query = {"name": {"$lte": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name <= "Hoima, Uganda"),))
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_ne_redis(redis_qparser):
    """$ne checks not equal in redis"""
    query = {"name": {"$ne": "Hoima, Uganda"}}
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name != "Hoima, Uganda"),)


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_ne_sql(sql_qparser):
    """$ne checks not equal in sql"""
    query = {"name": {"$ne": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name != "Hoima, Uganda"),))
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_nin_redis(redis_qparser):
    """$nin checks not in in redis"""
    query = {"name": {"$nin": ["Hoima, Uganda"]}}
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name >> ["Hoima, Uganda"]),)


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_nin_sql(sql_qparser):
    """$nin checks not in in sql"""
    query = {"name": {"$nin": ["Hoima, Uganda"]}}
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name.not_in(["Hoima, Uganda"])),))
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_not_redis(redis_qparser):
    """$not checks not in redis"""
    query = {"name": {"$not": {"$lt": "Hoima, Uganda"}}}
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == (~(RedisLibrary.name < "Hoima, Uganda"),)


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_not_sql(sql_qparser):
    """$not checks not in sql"""
    query = {"name": {"$not": {"$lt": "Hoima, Uganda"}}}
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, (~(SqlLibrary.name < "Hoima, Uganda"),))
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_regex_redis(redis_qparser):
    """$regex is not implemented for redis"""
    query = {"name": {"$regex": "^be.*", "$options": "i"}}
    with pytest.raises(
        NotImplementedError, match=r"redis text search is too inexpressive for regex.*"
    ):
        redis_qparser.to_redis(RedisLibrary, query)


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_regex_sql(sql_qparser):
    """$redis checks given item against a given regular expression"""
    query = {"name": {"$regex": "^be.*", "$options": "i"}}
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(
        SqlLibrary, ((SqlLibrary.name.regexp_match("(?i)^be.*", flags="i")),)
    )
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_and_redis(redis_qparser):
    """$and checks all conditions fulfilled in redis"""
    query = {
        "$and": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$or": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == (
        (RedisLibrary.name < "Hoima, Uganda")
        & ((RedisLibrary.address == "Bar") | (RedisLibrary.name > "Buliisa")),
    )


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_and_sql(sql_qparser):
    """$and checks all conditions fulfilled in sql"""
    query = {
        "$and": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$or": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(
        SqlLibrary,
        (
            (SqlLibrary.name < "Hoima, Uganda")
            & ((SqlLibrary.address == "Bar") | (SqlLibrary.name > "Buliisa")),
        ),
    )
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_or_redis(redis_qparser):
    """$or checks any condition fulfilled in redis"""
    query = {
        "$or": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$and": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == (
        (RedisLibrary.name < "Hoima, Uganda")
        | ((RedisLibrary.address == "Bar") & (RedisLibrary.name > "Buliisa")),
    )


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_or_sql(sql_qparser):
    """$or checks any condition fulfilled in sql"""
    query = {
        "$or": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$and": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(
        SqlLibrary,
        (
            (SqlLibrary.name < "Hoima, Uganda")
            | ((SqlLibrary.address == "Bar") & (SqlLibrary.name > "Buliisa")),
        ),
    )
    assert got == expected


@pytest.mark.skipif(not is_lib_installed("redis_om"), reason="Requires redis_om.")
def test_nor_redis(redis_qparser):
    """$nor checks none of the conditions is fulfilled in redis"""
    query = {
        "$nor": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$or": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = redis_qparser.to_redis(RedisLibrary, query)
    assert got == (
        ~(RedisLibrary.name < "Hoima, Uganda")
        & ~((RedisLibrary.address == "Bar") | (RedisLibrary.name > "Buliisa")),
    )


@pytest.mark.skipif(not is_lib_installed("sqlmodel"), reason="Requires sqlmodel.")
def test_nor_sql(sql_qparser):
    """$and checks none of the conditions is fulfilled in sql"""
    query = {
        "$nor": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$or": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = to_sql_text(SqlLibrary, sql_qparser.to_sql(SqlLibrary, query))
    expected = to_sql_text(
        SqlLibrary,
        (
            ~(
                (SqlLibrary.name < "Hoima, Uganda")
                | ((SqlLibrary.address == "Bar") | (SqlLibrary.name > "Buliisa"))
            ),
        ),
    )
    assert got == expected
