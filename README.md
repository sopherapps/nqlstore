# NQLStore

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

- [Python +3.11](https://www.python.org/downloads/)
- [Pydantic +2.0](https://docs.pydantic.dev/latest/)
- [SQLModel _(_optional_)](https://sqlmodel.tiangolo.com/) - only required for relational databases
- [RedisOM (_optional_)](https://redis.io/docs/latest/integrate/redisom-for-python/) - only required for [redis]()
- [Motor (_optional_)](https://www.mongodb.com/docs/drivers/motor/) - only required for [MongoDB]()

## Quick Start

### Install NQLStore from Pypi 

Install NQLStore from pypi, with any of the options: `all`, `sql`, `mongo`, `redis`.

```shell
pip install nqlstore
```

### Swap out your object mapping (OM) package imports with nqlstore

In your python modules, define your data models as you would define them with your favourite OM package.
**The only difference is the package you import them from.**

Here are examples of OM packages to substitute.

#### SQL

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

#### Redis

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

#### Mongo

```python
# models.py

from nqlstore.mongo import MongoModel 
    
class Library(MongoModel):
    id: str
    address: str
    name: str 

class Book(MongoModel):
    title: str 
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
    store = SQLStore(uri="sqlite:///database.db")
    store.init([
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
    store.init([
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
    store = MongoStore(uri="mongodb://localhost:27017")
    store.init([
        Library,
        Book,
    ])
```

### Use your models in your application

In the rest of you application use the four class methods available on the models.
Querying styles native to the different database technologies are supported out of the box.

For instance:

#### SQL

models based on SQL will follow the [SQLModel way of querying/filtering](https://sqlmodel.tiangolo.com/tutorial/where/#where-and-expressions-instead-of-keyword-arguments)

TODO: Complete design
```python
# app.py

from .models import Library, Book

def do_something():
    new_library = Library.create()
```
