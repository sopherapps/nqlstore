"""Schemas for the application"""

from pydantic import BaseModel

from nqlstore import Field, Relationship


class TodoList(BaseModel):
    """A list of Todos"""

    name: str = Field(index=True, full_text_search=True)
    todos: list["Todo"] = Relationship(back_populates="parent", default=[])


class Todo(BaseModel):
    """A single todo Item"""

    title: str = Field(index=True)
    is_complete: str = Field(default="0")
    parent_id: int | None = Field(
        default=None,
        foreign_key="sqltodolist.id",
        disable_on_mongo=True,
        disable_on_redis=True,
    )
    parent: TodoList | None = Relationship(
        back_populates="todos",
        disable_on_mongo=True,
        disable_on_redis=True,
    )
