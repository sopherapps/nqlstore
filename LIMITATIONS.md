# Limitations

## Filtering

### Redis

- Mongo-style regular expression filtering is not supported. 
  This is because native redis regular expression filtering is limited to the most basic text based search.

## Update Operation

### SQL 

- Even though one can update a model to theoretically infinite number of levels deep,
  the returned results can only contain 1-level-deep nested models and no more.
