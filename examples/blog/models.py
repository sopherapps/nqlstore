"""Models that are saved in storage"""

from schemas import Author, Comment, InternalAuthor, Post, Tag, TagLink

from nqlstore import (
    EmbeddedJsonModel,
    EmbeddedMongoModel,
    JsonModel,
    MongoModel,
    SQLModel,
)

# mongo models
MongoInternalAuthor = MongoModel("MongoInternalAuthor", InternalAuthor)
MongoAuthor = EmbeddedMongoModel("MongoAuthor", Author)
MongoComment = EmbeddedMongoModel(
    "MongoComment", Comment, embedded_models={"author": MongoAuthor}
)
MongoTag = EmbeddedMongoModel("MongoTag", Tag)
MongoPost = MongoModel(
    "MongoPost",
    Post,
    embedded_models={
        "author": MongoAuthor | None,
        "comments": list[MongoComment],
        "tags": list[MongoTag],
    },
)


# redis models
RedisInternalAuthor = JsonModel("RedisInternalAuthor", InternalAuthor)
RedisAuthor = EmbeddedJsonModel("RedisAuthor", Author)
RedisComment = EmbeddedJsonModel(
    "RedisComment", Comment, embedded_models={"author": RedisAuthor}
)
RedisTag = EmbeddedJsonModel("RedisTag", Tag)
RedisPost = JsonModel(
    "RedisPost",
    Post,
    embedded_models={
        "author": RedisAuthor | None,
        "comments": list[RedisComment],
        "tags": list[RedisTag],
    },
)

# sqlite models
SqlInternalAuthor = SQLModel("SqlInternalAuthor", InternalAuthor)
SqlComment = SQLModel(
    "SqlComment", Comment, relationships={"author": SqlInternalAuthor | None}
)
SqlTagLink = SQLModel("SqlTagLink", TagLink)
SqlTag = SQLModel("SqlTag", Tag)
SqlPost = SQLModel(
    "SqlPost",
    Post,
    relationships={
        "author": SqlInternalAuthor | None,
        "comments": list[SqlComment],
        "tags": list[SqlTag],
    },
    link_models={"tags": SqlTagLink},
)
