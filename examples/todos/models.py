"""Models that are saved in storage"""

from schemas import Todo, TodoList

from nqlstore import (
    EmbeddedJsonModel,
    EmbeddedMongoModel,
    JsonModel,
    MongoModel,
    SQLModel,
)

# mongo models
MongoTodo = EmbeddedMongoModel("MongoTodo", Todo)
MongoTodoList = MongoModel(
    "MongoTodoList", TodoList, embedded_models={"todos": list[MongoTodo]}
)

# redis models
RedisTodo = EmbeddedJsonModel("RedisTodo", Todo)
RedisTodoList = JsonModel(
    "RedisTodoList", TodoList, embedded_models={"todos": list[RedisTodo]}
)

# sqlite models
SqlTodoList = SQLModel(
    "SqlTodoList", TodoList, relationships={"todos": list["SqlTodo"]}
)
SqlTodo = SQLModel("SqlTodo", Todo, relationships={"parent": SqlTodoList | None})
