"""Tests for the query package"""

from tests.conftest import RedisLibrary, SqlLibrary
from tests.utils import to_sql_text


def test_eq_redis(query_parser):
    """$eq checks equality in redis"""
    query = {"name": {"$eq": "Hoima, Uganda"}}
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name == "Hoima, Uganda"),)


def test_eq_sql(query_parser):
    """$eq checks equality in sql"""
    query = {"name": {"$eq": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name == "Hoima, Uganda"),))
    assert got == expected


def test_gt_redis(query_parser):
    """$gt checks greater than in redis"""
    query = {"name": {"$gt": "Hoima, Uganda"}}
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name > "Hoima, Uganda"),)


def test_gt_sql(query_parser):
    """$gt checks greater than in sql"""
    query = {"name": {"$gt": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name > "Hoima, Uganda"),))
    assert got == expected


def test_gte_redis(query_parser):
    """$gte checks greater or equal in redis"""
    query = {"name": {"$gte": "Hoima, Uganda"}}
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name >= "Hoima, Uganda"),)


def test_gte_sql(query_parser):
    """$gte checks greater or equal in sql"""
    query = {"name": {"$gte": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name >= "Hoima, Uganda"),))
    assert got == expected


def test_in_redis(query_parser):
    """$in checks value in list in redis"""
    query = {"name": {"$in": ["Hoima, Uganda"]}}
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name << ["Hoima, Uganda"]),)


def test_in_sql(query_parser):
    """$in checks vlue in list than in sql"""
    query = {"name": {"$in": ["Hoima, Uganda"]}}
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name.in_(["Hoima, Uganda"])),))
    assert got == expected


def test_lt_redis(query_parser):
    """$lt checks less than in redis"""
    query = {"name": {"$lt": "Hoima, Uganda"}}
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name < "Hoima, Uganda"),)


def test_lt_sql(query_parser):
    """$lt checks less than in sql"""
    query = {"name": {"$lt": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name < "Hoima, Uganda"),))
    assert got == expected


def test_lte_redis(query_parser):
    """$lt checks less or equal in redis"""
    query = {"name": {"$lte": "Hoima, Uganda"}}
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name <= "Hoima, Uganda"),)


def test_lte_sql(query_parser):
    """$lt checks less or equal in sql"""
    query = {"name": {"$lte": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name <= "Hoima, Uganda"),))
    assert got == expected


def test_ne_redis(query_parser):
    """$ne checks not equal in redis"""
    query = {"name": {"$ne": "Hoima, Uganda"}}
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name != "Hoima, Uganda"),)


def test_ne_sql(query_parser):
    """$ne checks not equal in sql"""
    query = {"name": {"$ne": "Hoima, Uganda"}}
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name != "Hoima, Uganda"),))
    assert got == expected


def test_nin_redis(query_parser):
    """$nin checks not in in redis"""
    query = {"name": {"$nin": ["Hoima, Uganda"]}}
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == ((RedisLibrary.name >> ["Hoima, Uganda"]),)


def test_nin_sql(query_parser):
    """$nin checks not in in sql"""
    query = {"name": {"$nin": ["Hoima, Uganda"]}}
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, ((SqlLibrary.name.not_in(["Hoima, Uganda"])),))
    assert got == expected


def test_not_redis(query_parser):
    """$not checks not in redis"""
    query = {"name": {"$not": {"$lt": "Hoima, Uganda"}}}
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == (~(RedisLibrary.name < "Hoima, Uganda"),)


def test_not_sql(query_parser):
    """$not checks not in sql"""
    query = {"name": {"$not": {"$lt": "Hoima, Uganda"}}}
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(SqlLibrary, (~(SqlLibrary.name < "Hoima, Uganda"),))
    assert got == expected


def test_and_redis(query_parser):
    """$and checks all conditions fulfilled in redis"""
    query = {
        "$and": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$or": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == (
        (RedisLibrary.name < "Hoima, Uganda")
        & ((RedisLibrary.address == "Bar") | (RedisLibrary.name > "Buliisa")),
    )


def test_and_sql(query_parser):
    """$and checks all conditions fulfilled in sql"""
    query = {
        "$and": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$or": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(
        SqlLibrary,
        (
            (SqlLibrary.name < "Hoima, Uganda")
            & ((SqlLibrary.address == "Bar") | (SqlLibrary.name > "Buliisa")),
        ),
    )
    assert got == expected


def test_or_redis(query_parser):
    """$or checks any condition fulfilled in redis"""
    query = {
        "$or": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$and": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == (
        (RedisLibrary.name < "Hoima, Uganda")
        | ((RedisLibrary.address == "Bar") & (RedisLibrary.name > "Buliisa")),
    )


def test_or_sql(query_parser):
    """$or checks any condition fulfilled in sql"""
    query = {
        "$or": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$and": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
    expected = to_sql_text(
        SqlLibrary,
        (
            (SqlLibrary.name < "Hoima, Uganda")
            | ((SqlLibrary.address == "Bar") & (SqlLibrary.name > "Buliisa")),
        ),
    )
    assert got == expected


def test_nor_redis(query_parser):
    """$nor checks none of the conditions is fulfilled in redis"""
    query = {
        "$nor": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$or": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = query_parser.to_redis(RedisLibrary, query)
    assert got == (
        ~(RedisLibrary.name < "Hoima, Uganda")
        & ~((RedisLibrary.address == "Bar") | (RedisLibrary.name > "Buliisa")),
    )


def test_nor_sql(query_parser):
    """$and checks none of the conditions is fulfilled in sql"""
    query = {
        "$nor": [
            {"name": {"$lt": "Hoima, Uganda"}},
            {"$or": [{"address": {"$eq": "Bar"}}, {"name": {"$gt": "Buliisa"}}]},
        ]
    }
    got = to_sql_text(SqlLibrary, query_parser.to_sql(SqlLibrary, query))
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
