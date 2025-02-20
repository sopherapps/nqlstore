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
        "author": MongoAuthor,
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
        "author": RedisAuthor,
        "comments": list[RedisComment],
        "tags": list[RedisTag],
    },
)

# sqlite models
SqlInternalAuthor = SQLModel("SqlInternalAuthor", InternalAuthor)
SqlAuthor = SQLModel("SqlAuthor", Author, table=False)
SqlComment = SQLModel("SqlComment", Comment, relationships={"author": SqlAuthor | None})
SqlTagLink = SQLModel("SqlTagLink", TagLink)
SqlTag = SQLModel("SqlTag", Tag)
SqlPost = SQLModel(
    "SqlPost",
    Post,
    relationships={
        "author": SqlAuthor | None,
        "comments": list[SqlComment],
        "tags": list[SqlTag],
    },
)
