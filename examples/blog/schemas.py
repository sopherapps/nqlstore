"""Schemas for the application"""

from datetime import datetime, timezone

from pydantic import BaseModel

from nqlstore import Field, Relationship


class Author(BaseModel):
    """The author as returned to the user"""

    name: str = Field(index=True, full_text_search=True)


class InternalAuthor(Author):
    """The author as saved in database"""

    password: str = Field()
    email: str = Field(index=True)


class Post(BaseModel):
    """The post"""

    title: str = Field(index=True, full_text_search=True)
    content: str | None = Field()
    author_id: int | None = Field(
        default=None,
        foreign_key="sqlauthor.id",
        disable_on_mongo=True,
        disable_on_redis=True,
    )
    author: Author | None = Relationship(default=None)
    comments: list["Comment"] = Relationship(
        default=[],
        disable_on_redis=True,
    )
    tags: list["Tag"] = Relationship(
        default=[],
        link_model="TagLink",
        disable_on_redis=True,
    )
    created_at: datetime = Field(index=True, default_factory=datetime.now)
    updated_at: datetime = Field(index=True, default_factory=datetime.now)


class Comment(BaseModel):
    """The comment on a post"""

    post_id: int | None = Field(
        default=None,
        foreign_key="sqlpost.id",
        disable_on_mongo=True,
        disable_on_redis=True,
    )
    content: str | None = Field()
    author_id: int | None = Field(
        default=None,
        foreign_key="sqlauthor.id",
        disable_on_mongo=True,
        disable_on_redis=True,
    )
    author: Author | None = Relationship(default=None)
    created_at: datetime = Field(index=True, default_factory=datetime.now)
    updated_at: datetime = Field(index=True, default_factory=datetime.now)


class TagLink(BaseModel):
    """The SQL-only join table between tags and posts"""

    post_id: int | None = Field(
        default=None,
        foreign_key="sqlpost.id",
        primary_key=True,
        disable_on_mongo=True,
        disable_on_redis=True,
    )
    tag_id: int | None = Field(
        default=None,
        foreign_key="sqltag.id",
        primary_key=True,
        disable_on_mongo=True,
        disable_on_redis=True,
    )


class Tag(BaseModel):
    """The tags to help searching for posts"""

    title: str = Field(index=True, unique=True, full_text_search=True)


class TokenResponse(BaseModel):
    """HTTP-only response"""

    access_token: str
    token_type: str
