# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

### Fixed

- Fixed Type annotations for `SQLModel`, `MongoModel`, `JsonModel`, `HashModel` and `EmbeddedJsonModel`

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
