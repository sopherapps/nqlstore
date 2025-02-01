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

## Quick Start

### Install NQLStore from Pypi 

Install NQLStore from pypi, with any of the options: `sql`, `mongo`, `redis`, `all`.

```shell
pip install nqlstore
```

### Swap out your object mapping (OM) package imports with nqlstore

In your python modules, define your data models as you would define them with your favourite OM package.
**The only difference is the package you import them from.**

Here are examples of OM packages to substitute.

#### SQL (use [SQLModel](https://sqlmodel.tiangolo.com/) models)

```python
# models.py

from nqlstore.sql import Field, SQLModel 

class Library(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    address: str 
    name: str 
    
class Book(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    title: str 
    library_id: int = Field(default=None, foreign_key="library.id")
```

#### Redis (use [RedisOM](https://redis.io/docs/latest/integrate/redisom-for-python/) models)

**Take note that JsonModel, EmbeddedJsonModel require RedisJSON, while queries require RedisSearch to be loaded**
**You need to install [redis-stack](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/) or load the modules manually**

```python
# models.py
from typing import List 

from nqlstore.redis import Field, JsonModel, EmbeddedJsonModel

class Book(EmbeddedJsonModel):
    title: str = Field(index=True)
    
class Library(JsonModel):
    address: str 
    name: str = Field(index=True)
    books: List[Book]
```

#### Mongo (use [Beanie](https://beanie-odm.dev/))

```python
# models.py

from nqlstore.mongo import Document, Indexed
    
class Library(Document):
    address: str
    name: str 


class Book(Document):
    title: Indexed(str) 
    library_id: str
```

### Initialize your store

Initialize the store that is to host your models.
Similar to how you imported models from specific packages in `nqlstore`,
import stores from the appropriately named modules in`nqlstore`.

Here are examples for the different database technologies.

#### SQL

_Migrations are outside the scope of this package_

```python
# main.py

from nqlstore.sql import SQLStore
from .models import Book, Library

if __name__ == "__main__":
    store = SQLStore(uri="sqlite+aiosqlite:///database.db")
    store.register([
        Library,
        Book,
    ])
```

#### Redis

```python
# main.py

from nqlstore.redis import RedisStore
from .models import Book, Library

if __name__ == "__main__":
    store = RedisStore(uri="rediss://username:password@localhost:6379/0")
    store.register([
        Library,
        Book,
    ])
```

#### Mongo

```python
# main.py

from nqlstore.mongo import MongoStore
from .models import Book, Library

if __name__ == "__main__":
    store = MongoStore(uri="mongodb://localhost:27017", database="testing")
    store.register([
        Library,
        Book,
    ])
```

### Use your models in your application

In the rest of you application use the four class methods available on the models.
Filtering follows the [MongoDb-style](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/#find-documents-that-match-query-criteria)

However, for more complex queries, one can also pass in querying styles native to the type of the database,
alongside the MongoBD-style querying. The two queries would be merged as `AND` queries.  

Or one can simply ignore the MongoDB-style querying and stick to the native querying.  

The available querying formats include:

- SQL - [SQLModel-style](https://sqlmodel.tiangolo.com/tutorial/where/#where-and-expressions-instead-of-keyword-arguments)
- Redis [RedisOM-style](https://redis.io/docs/latest/integrate/redisom-for-python/#create-read-update-and-delete-data)
- MongoDb [MongoDB-style](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/#find-documents-that-match-query-criteria)

#### Insert

Inserting new items in a store, call `store.insert(Type[Model], List[dict])` method.

```python
new_libraries = await store.insert(Library, [{}, {}])
```

#### Find

Finding items in a store, call `store.find(Type[Model], *filters: Any, skip: int=0, limit: int | None=None)` method.

The key-word arguments include:

- `skip (int)` - number of records to ignore at the top of the returned results; default is 0.
- `limit (int | None)` - maximum number of records to return; default is None.

The querying format is as described [above](#use-your-models-in-your-application)   

##### SQL filtering is SQLModel-style

###### MongoDB-style:

```python
libraries = await store.find(
    Library, nql_query={"name": {"$eq": "Hairora"}, "address" : {"$ne": "Buhimba"}}
)
```

###### Native-style only:

```python
libraries = await store.find(
    Library, Library.name == "Hairora", Library.address != "Buhimba"
)
```

###### Hybrid

```python
libraries = await store.find(
    Library, Library.name == "Hairora", nql_query={"address" : {"$ne": "Buhimba"}}
)
```

##### Redis filtering is RedisOM-style

###### MongoDB-style:

```python
libraries = await store.find(
    Library, nql_query={"name": {"$eq": "Hairora"}, "address" : {"$ne": "Buhimba"}}
)
```

###### Native-style only:

```python
libraries = await store.find(
    Library, (Library.name == "Hairora") & (Library.address != "Buhimba")
)
```

###### Hybrid

```python
libraries = await store.find(
    Library, (Library.name == "Hairora"), nql_query={"address" : {"$ne": "Buhimba"}}
)
```

##### Mongo filtering is MongoDB-style

```python
libraries = await store.find(
    Library, {"name": "Hairora", "address": {"$ne": "Buhimba"}}
)
```

#### Update

Updating items in a store, call `store.update(model: Type[Model], *filters: Any, updates: dict)` method.

The method returns the newly updated records.  
The `filters` follow the same style as that used when querying as shown [above](#read).  
Similarly, `updates` are different for each type of database technology as alluded to [earlier](#use-your-models-in-your-application).

##### SQL updates are just dictionaries of the new field values

###### MongoDB-style:

```python
libraries = await store.update(
    Library, 
    nql_query={"name": {"$eq": "Hairora"}, "address" : {"$ne": "Buhimba"}},
    updates={"name": "Foo"},
)
```

###### Native-style only:

```python
libraries = await store.update(
    Library, 
    Library.name == "Hairora", Library.address != "Buhimba", 
    updates={"name": "Foo"},
)
```

###### Hybrid

```python
libraries = await store.update(
    Library, 
    Library.name == "Hairora", nql_query={"address" : {"$ne": "Buhimba"}},
    updates={"name": "Foo"},
)
```

##### Redis updates are just dictionaries of the new field values

###### MongoDB-style:

```python
libraries = await store.update(
    Library, 
    nql_query={"name": {"$eq": "Hairora"}, "address" : {"$ne": "Buhimba"}},
    updates={"name": "Foo"},
)
```

###### Native-style only:

```python
libraries = await store.update(
    Library, 
    (Library.name == "Hairora") & (Library.address != "Buhimba"), 
    updates={"name": "Foo"},
)
```

###### Hybrid

```python
libraries = await store.update(
    Library, 
    (Library.name == "Hairora"), 
    nql_query={"address" : {"$ne": "Buhimba"}},
    updates={"name": "Foo"},
)
```


##### Mongo updates are [MongoDB-style update dicts](https://www.mongodb.com/docs/manual/reference/operator/update/)

```python
libraries = await store.update(
    Library,
    {"name": "Hairora", "address": {"$ne": "Buhimba"}},
    updates={"$set": {"name": "Foo"}},
)
```

#### Delete

Deleting items in a store, call `store.delete(model: Type[Model], *filters: Any)` method.

The `filters` follow the same style as that used when reading as shown [above](#read).  

##### SQL filtering is SQLModel-style

###### MongoDB-style:

```python
libraries = await store.delete(
    Library, nql_query={"name": {"$eq": "Hairora"}, "address" : {"$ne": "Buhimba"}}
)
```

###### Native-style only:

```python
libraries = await store.delete(
    Library, Library.name == "Hairora", Library.address != "Buhimba"
)
```

###### Hybrid

```python
libraries = await store.delete(
    Library, Library.name == "Hairora", nql_query={"address" : {"$ne": "Buhimba"}}
)
```

##### Redis filtering is RedisOM-style

###### MongoDB-style:

```python
libraries = await store.delete(
    Library, nql_query={"name": {"$eq": "Hairora"}, "address" : {"$ne": "Buhimba"}}
)
```

###### Native-style only:

```python
libraries = await store.delete(
    Library, (Library.name == "Hairora") & (Library.address != "Buhimba")
)
```

###### Hybrid

```python
libraries = await store.delete(
    Library, (Library.name == "Hairora"), nql_query={"address" : {"$ne": "Buhimba"}}
)
```

##### Mongo filtering is MongoDB-style

```python
libraries = await store.delete(
  Library, {"name": "Hairora", "address": {"$ne": "Buhimba"}}
)
```

## TODO

- [ ] Add test for each selector, testing the final output of the SQL filters, redis filters, mongo filters

## License

Copyright (c) 2025 [Martin Ahindura](https://github.com/Tinitto)   
Licensed under the [MIT License](./LICENSE)

## Gratitude

> "In that day you will ask in My name. I am not saying that I will ask the Father on your behalf.
> No, the Father himself loves you because you have loved Me and have believed that I came from God."
>
> -- John 16: 26-27

All glory be to God

<a href="https://www.buymeacoffee.com/martinahinJ" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" ></a>