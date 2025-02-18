# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [0.1.5] - 2025-02-18

### Added

- Added [todos](./examples/todos) example application

### Changed

- Replaced the second required argument of `MongoStore.find()`, 
  `MongoStore.update()`, `MongoStore.delete()`with their first optional 
  key-word argument `query`.
- Changed the `limit` parameter of the `MongoStore.find()` to be by default 0.
- Removed the `Beanie-odm`-specific arguments passed to `MongoStore.insert()`,
  `MongoStore.find()`, `MongoStore.update()`, `MongoStore.delete()`.

### Fixed

- Fixed error: "got Future <Future pending> attached to a different loop" when
  `MongoStore.insert()`, `MongoStore.find()`, `MongoStore.update()`, `MongoStore.delete()`
  are called especially during tests that create different event loops.
- Fixed `SQLStore.delete()` to return any nested items that were deleted.
- Fixed error: "got Future <Future pending> attached to a different loop" when
  `RedisStore.insert()`, `RedisStore.find()`, `RedisStore.update()`, `RedisStore.delete()`
  are called especially during tests that create different event loops.

## [0.1.4] - 2025-02-15

### Fixed

- Enabled recursive serialization of child SQL models when `model_dump()` is called.

## [0.1.3] - 2025-02-15

### Added

- Added ability to insert children of parent models as though they were embedded in the parent 
  in SQL the SQL implementation. (This makes it possible to simulate embeddedness)
- Added ability to replace children of SQL parent models when updating the parent

## [0.1.2] - 2025-02-15

### Added

- Added an `id` field to the Redis models to shadow the `pk` field

### Changed

- Changed the module name of the Model classes created to equal the module they are called from

### Fixed

- Fixed types for the `embedded_models` parameter of the `MongoModel()` and `EmbeddedModel()` functions

## [0.1.1] - 2025-02-14

### Changed

- Improved the type annotations of the created models to show both Schema 
  and db-specific model classes like "Document"

## [0.1.0] - 2025-02-13

### Added

- Added Mongo-like Dot notation for querying SQL and redis

### Changed

- Added the `query` key-word parameter to the `find`, `update`, and `delete` for the mongo implementation
  so that it is similar to the other interfaces
- Added the `embedded_models` key-word argument on the `Model` initializers for redis and mongodb
- Added the `relationships` key-word argument to the `Model` initializers for SQL

## [0.0.3] - 2025-02-06

### Changed

- Changed Github actions to run tests for each extra plus the `test` extra i.e. `[sql,test]`, `[redis,test]` etc. 

### Fixed

- Fixed Type annotations for `SQLModel`, `MongoModel`, `JsonModel`, `HashModel` and `EmbeddedJsonModel`
- Fixed import errors when only `sql` or `redis` or `mongo` extras are installed.

## [0.0.2] - 2025-02-06

### Added

- Added regex support for SQL

### Changed

- Allowed update dicts without operators in mongo update implementation
- Enabled defining schemas once to be shared by different database technologies
- Disabled direct imports from `nqlstore.mongo`, `nqlstore.sql`, `nqlstore.redis`

## [0.0.1] - 2025-02-04

### Added

- Added support for redis
- Added support for MongoDB
- Added support for all SQL databases supported by [sqlalchemy](https://www.sqlalchemy.org/)
- Added a unified query format compatible with mongodb-style querying
- Added support for using both Mongo-like query format with the native query 
  format of the underlying database technology

### Changed

### Fixed
