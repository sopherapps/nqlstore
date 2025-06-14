# Blog

A simple Blog app based on [NQLStore](https://github.com/sopherapps/nqlstore)

## Requirements

- [Python +3.10](https://python.org)
- [NQLStore](https://github.com/sopherapps/nqlstore)
- [Redis stack (optional)](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/)
- [MongoDB (optional)](https://www.mongodb.com/products/self-managed/community-edition)
- [SQLite (optional)](https://www.sqlite.org/)

## Getting Started

- Ensure you have [Python +3.10](https://python.org) installed

- Copy this repository and enter this folder

```shell
git clone https://github.com/sopherapps/nqlstore.git
cd nqlstore/examples/blog
```

- Create a virtual env, activate it and install requirements

```shell
python -m venv env 
source env/bin/activate
pip install -r requirements.txt
```

- To use with [MongoDB](https://www.mongodb.com/try/download/community), install and start its server.
- To use with redis, install [redis stack](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/) 
  and start its server in another terminal.

- Start the application, set the URL's for the database(s) to use.  
  Options are:
  - `SQL_URL` for [SQLite](https://www.sqlite.org/).
  - `MONGO_URL` (required) and `MONGO_DB` (default: "todos") for [MongoDB](https://www.mongodb.com/products/self-managed/community-edition)
  - `REDIS_URL` for [Redis](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/).
  
  _It is possible to use multiple databases at the same time. Just set multiple environment variables_

```shell
export SQL_URL="sqlite+aiosqlite:///test.db"
#export MONGO_URL="mongodb://localhost:27017"
#export MONGO_DB="testing"
export REDIS_URL="redis://localhost:6379/0"
fastapi dev main.py
```

## License

Copyright (c) 2025 [Martin Ahindura](https://github.com/Tinitto)   
Licensed under the [MIT License](./LICENSE)

## Gratitude

Glory be to God for His unmatchable love.

> "As Jesus was on His way, the crowds almost crushed Him.
> And a woman was there who had been subject to bleeding
> for twelve years, but no one could heal her.
> She came up behind Him and touched the edge of His cloak,
> and immediately her bleeding stopped."
>
> -- Luke 8: 42-44

<a href="https://www.buymeacoffee.com/martinahinJ" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" ></a>
