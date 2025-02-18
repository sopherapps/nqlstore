# NQLStore

[![PyPI version](https://badge.fury.io/py/nqlstore.svg)](https://badge.fury.io/py/nqlstore) ![CI](https://github.com/sopherapps/nqlstore/actions/workflows/ci.yml/badge.svg)

NQLStore, a simple CRUD store python library for `any query launguage` (or in short `nql`)

---

NQLStore provides an oversimplified API for the mundane things of _creating_, _reading_,
_updating_, and _deleting_ data models that are persisted to any `SQL-` or `NoSQL-` database. 

In total, all we need are four methods and that is it.

Supported databases include:

- Relational databases like:

  - SQLite
  - PostgreSQL
  - MySQL

- NoSQL databases like:

  - Redis
  - MongoDB

If you like our simple API, you can even easily extend it to 
support your favourite database technology.

## Dependencies

- [Python +3.10](https://www.python.org/downloads/)
- [Pydantic +2.0](https://docs.pydantic.dev/latest/)
- [SQLModel _(_optional_)](https://sqlmodel.tiangolo.com/) - only required for relational databases
- [RedisOM (_optional_)](https://redis.io/docs/latest/integrate/redisom-for-python/) - only required for [redis](https://redis.io/)
- [Beanie (_optional_)](https://beanie-odm.dev/) - only required for [MongoDB](https://www.mongodb.com/)

## Examples

See the [`examples`](/examples) folder for some example applications.  
Hopefully more examples will be added with time. Currently, we have the following:

- [todos](./examples/todos)

## Quick Start

### Install NQLStore from Pypi 

Install NQLStore from pypi, with any of the options: `sql`, `mongo`, `redis`, `all`.

```shell
pip install "nqlstore[all]"
```

### Create Schemas

Create the basic structure of the data that will be saved in the store.  
These schemas will later be used to create models that are specific to the underlying database
technology.

```python
# schemas.py

from nqlstore import Field, Relationship
from pydantic import BaseModel


class Library(BaseModel):
    address: str = Field(index=True, full_text_search=True)
    name: str = Field(index=True, full_text_search=True)
    books: list["Book"] = Relationship(back_populates="library")

    class Settings:
        # this Settings class is optional. It is only used by Mongo models
        # See https://beanie-odm.dev/tutorial/defining-a-document/
        name = "libraries"


class Book(BaseModel):
    title: str = Field(index=True)
    library_id: int | None = Field(default=None, foreign_key="sqllibrary.id", disable_on_redis=True, disable_on_mongo=True)
    library: Library | None = Relationship(back_populates="books", disable_on_redis=True, disable_on_mongo=True)
```

### Initialize your store and its models

Initialize the store and its models that is to host your models.

#### SQL

_Migrations are outside the scope of this package_

```python
# main.py

from nqlstore import SQLStore, SQLModel
from .schemas import Book, Library


# Define models specific to SQL.
SqlLibrary = SQLModel(
        "SqlLibrary", Library, relationships={"books": list["SqlBook"]}
    )
SqlBook = SQLModel("SqlBook", Book, relationships={"library": SqlLibrary | None})



async def main():
  sql_store = SQLStore(uri="sqlite+aiosqlite:///database.db")
  await sql_store.register([
    SqlLibrary,
    SqlBook,
  ])
```

#### Redis

**Take note that JsonModel, EmbeddedJsonModel require RedisJSON, while queries require RedisSearch to be loaded**
**You need to install [redis-stack](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/) or load the modules manually**

```python
# main.py

from nqlstore import RedisStore, EmbeddedJsonModel, JsonModel
from .schemas import Book, Library

# Define models specific to redis.
RedisBook = EmbeddedJsonModel("RedisBook", Book)
RedisLibrary = JsonModel("RedisLibrary", Library, embedded_models={"books": list[RedisBook]})

async def main():
  redis_store = RedisStore(uri="rediss://username:password@localhost:6379/0")
  await redis_store.register([
    RedisLibrary,
    RedisBook,
  ])
```

#### Mongo

```python
# main.py

from nqlstore import MongoStore, MongoModel, EmbeddedMongoModel
from .schemas import Library, Book

# Define models specific to MongoDB.
MongoBook = EmbeddedMongoModel("MongoBook", Book)
MongoLibrary = MongoModel("MongoLibrary", Library, embedded_models={"books": list[MongoBook]})


async def main():
  mongo_store = MongoStore(uri="mongodb://localhost:27017", database="testing")
  await mongo_store.register([
    MongoLibrary,
    MongoBook,
  ])

```

### Use your models in your application

In the rest of your application use the four CRUD methods on the store to do CRUD operations.  
Filtering follows the [MongoDb-style](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/#find-documents-that-match-query-criteria)

> **Note**: For more complex queries, one can also pass in querying styles native to the type of the database,  
> alongside the MongoBD-style querying. The two queries would be merged as `AND` queries.  
>
> Or one can simply ignore the MongoDB-style querying and stick to the native querying.  
> 
> The available querying formats include:
> 
> - SQL - [SQLModel-style](https://sqlmodel.tiangolo.com/tutorial/where/#where-and-expressions-instead-of-keyword-arguments)
> - Redis - [RedisOM-style](https://redis.io/docs/latest/integrate/redisom-for-python/#create-read-update-and-delete-data)
> - MongoDb - [MongoDB-style](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/#find-documents-that-match-query-criteria)

#### Insert

Inserting new items in a store, call `store.insert()` method.

```python
new_libraries = await sql_store.insert(SqlLibrary, [{}, {}])
new_libraries = await mongo_store.insert(MongoLibrary, [{}, {}])
new_libraries = await redis_store.insert(RedisLibrary, [{}, {}])
```

#### Find

Finding items in a store, call the `store.find()` method.

The key-word arguments include:

- `skip (int)` - number of records to ignore at the top of the returned results; default is 0.
- `limit (int | None)` - maximum number of records to return; default is None.

The querying format is as described [above](#use-your-models-in-your-application)

```python
# MongoDB-style: works with any underlying database technology
libraries = await sql_store.find(
    SqlLibrary, query={"name": {"$eq": "Hairora"}, "address" : {"$ne": "Buhimba"}}
)


# Native SQL-style: works only if underlying database is SQL database
libraries = await sql_store.find(
    SqlLibrary, SqlLibrary.name == "Hairora", SqlLibrary.address != "Buhimba"
)


# Hybrid SQL-Mongo-style: works only if underlying database is SQL database
libraries = await sql_store.find(
    SqlLibrary, SqlLibrary.name == "Hairora", query={"address" : {"$ne": "Buhimba"}}
)


# Native Redis-style: works only if underlying database is redis database
libraries = await redis_store.find(
    RedisLibrary, (RedisLibrary.name == "Hairora") & (RedisLibrary.address != "Buhimba")
)


# Hybrid redis-mongo-style: works only if underlying database is redis database
libraries = await redis_store.find(
    RedisLibrary, (RedisLibrary.name == "Hairora"), query={"address" : {"$ne": "Buhimba"}}
)
```

#### Update

Updating items in a store, call the `store.update()` method.

The method returns the newly updated records.  
The `filters` follow the same style as that used when querying as shown [above](#read). 

The `updates` objects are simply dictionaries of the new field values.

```python
# Mongo-style of filtering: works with any underlying database technology
libraries = await redis_store.update(
    RedisLibrary, 
    query={"name": {"$eq": "Hairora"}, "address" : {"$ne": "Buhimba"}},
    updates={"name": "Foo"},
)


# Native SQL-style filtering: works only if the underlying database is SQL
libraries = await sql_store.update(
    SqlLibrary, 
    SqlLibrary.name == "Hairora", SqlLibrary.address != "Buhimba", 
    updates={"name": "Foo"},
)


# Hybrid SQL-mongo-style filtering: works only if the underlying database is SQL
libraries = await sql_store.update(
    SqlLibrary, 
    SqlLibrary.name == "Hairora", query={"address" : {"$ne": "Buhimba"}},
    updates={"name": "Foo"},
)


# Native redisOM-style filtering: works only if the underlying database is redis
libraries = await redis_store.update(
    RedisLibrary, 
    (RedisLibrary.name == "Hairora") & (RedisLibrary.address != "Buhimba"), 
    updates={"name": "Foo"},
)


# Hybrid redis-mongo-style filtering: works only if the underlying database is redis
libraries = await redis_store.update(
    RedisLibrary, 
    (RedisLibrary.name == "Hairora"), 
    query={"address" : {"$ne": "Buhimba"}},
    updates={"name": "Foo"},
)


# MongoDB is special. It can also accept `updates` of the MongoDB-style update dicts
# See <https://www.mongodb.com/docs/manual/reference/operator/update/>.
# However, this has the disadvantage of making it difficult to swap out MongoDb 
# with another underlying technology.
#
# It is thus recommended to stick to using `updates` that are simply 
# dictionaries of the new field values.
# 
# The MongoDB-style update dicts work only if the underlying database is mongodb
libraries = await mongo_store.update(
    MongoLibrary,
    {"name": "Hairora", "address": {"$ne": "Buhimba"}},
    updates={"$set": {"name": "Foo"}}, # "$inc", "$addToSet" etc. can be accepted, but use with care
)

```


#### Delete

Deleting items in a store, call the `store.delete()` method.

The `filters` follow the same style as that used when reading as shown [above](#read).

```python
# Mongo-style of filtering: works with any underlying database technology
libraries = await mongo_store.delete(
    MongoLibrary, query={"name": {"$eq": "Hairora"}, "address" : {"$ne": "Buhimba"}}
)


# Native SQL-style filtering: works only if the underlying database is SQL
libraries = await sql_store.delete(
    SqlLibrary, SqlLibrary.name == "Hairora", SqlLibrary.address != "Buhimba"
)


# Hybrid SQL-mongo-style filtering: works only if the underlying database is SQL
libraries = await sql_store.delete(
    SqlLibrary, SqlLibrary.name == "Hairora", query={"address" : {"$ne": "Buhimba"}}
)


# Native redisOM-style filtering: works only if the underlying database is redis
libraries = await redis_store.delete(
    RedisLibrary, (RedisLibrary.name == "Hairora") & (RedisLibrary.address != "Buhimba")
)


# Hybrid redis-mongo-style filtering: works only if the underlying database is redis
libraries = await redis_store.delete(
    RedisLibrary, (RedisLibrary.name == "Hairora"), query={"address" : {"$ne": "Buhimba"}}
)
```

## TODO

- [ ] Add documentation site

## Contributions

Contributions are welcome. The docs have to maintained, the code has to be made cleaner, more idiomatic and faster,
and there might be need for someone else to take over this repo in case I move on to other things. It happens!

When you are ready, look at the [CONTRIBUTIONS GUIDELINES](./CONTRIBUTING.md)

## License

Copyright (c) 2025 [Martin Ahindura](https://github.com/Tinitto)   
Licensed under the [MIT License](./LICENSE)

## Gratitude

Thanks goes to the people in the [CREDITS.md](./CREDITS.md), for the efforts
they have put into this project.  

But above all, glory be to God.

> "In that day you will ask in My name. I am not saying that I will ask the Father on your behalf.
> No, the Father himself loves you because you have loved Me and have believed that I came from God."
>
> -- John 16: 26-27

<a href="https://www.buymeacoffee.com/martinahinJ" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" ></a>
