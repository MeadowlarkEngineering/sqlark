# SQLark

A lightweight Non-ORM with a builder pattern for querying a PostgreSQL database. SQLark provides a simple, intuitive API for building SQL queries while offering powerful response formatting options to transform database results into Python objects, including support for nested and related data structures.

## Quick Start

This quick start guide will help you get up and running with SQLark, from basic queries to advanced object mapping with nested relationships.

### Getting Started

### Installation

Using pip:
```sh
pip install git+https://github.com/MeadowlarkEngineering/sqlark.git#egg=sqlark
```

Using poetry:
```sh
poetry add git+https://github.com/MeadowlarkEngineering/sqlark.git
```

### Database Configuration

Set up your PostgreSQL connection:

```python
from sqlark import PostgresConfig

config = PostgresConfig(
    dbname="my_database",
    user="postgres_user",
    password="postgres_password",
    host="localhost",  # optional, defaults to localhost
    port=5432  # optional, defaults to 5432
)
```

> **Security Note**: In production environments, avoid hardcoding credentials. Use environment variables or secure configuration management instead:
> ```python
> import os
> config = PostgresConfig(
>     dbname=os.environ.get("DB_NAME"),
>     user=os.environ.get("DB_USER"),
>     password=os.environ.get("DB_PASSWORD")
> )
> ```

### Basic Queries

#### Simple SELECT

Select all records from a table:

```python
from sqlark import Select

result = Select("posts").execute(config)
# Returns: [{'posts.id': 1, 'posts.title': 'My First Post', 'posts.author_id': 1}, ...]
```

#### SELECT with WHERE, ORDER BY, and LIMIT

Build more complex queries using method chaining:

```python
result = Select("posts")\
    .where(column="author_id", operator="=", value=1)\
    .order_by("created_at", direction="DESC")\
    .limit(10)\
    .execute(config)
```

#### SELECT with Multiple Conditions

Combine multiple WHERE clauses:

```python
result = Select("posts")\
    .where(column="author_id", operator="=", value=1)\
    .where_and(column="published", operator="=", value=True)\
    .where_and(column="created_at", operator=">", value="2024-01-01")\
    .execute(config)
```

#### SELECT with JOINs

Join multiple tables together:

```python
result = Select("posts")\
    .join(
        right_table="authors",
        left_col="author_id",
        right_col="id"
    )\
    .where(table="authors", column="name", operator="=", value="John Doe")\
    .execute(config)
# Returns both posts.* and authors.* columns
```

> **Note**: The `left_table` parameter defaults to the primary table (the one specified in `Select()`), and `left_col` defaults to `"id"`. You can omit these parameters when joining from the primary table with its `id` column.

### INSERT, UPDATE, and DELETE

#### Insert Data

```python
from sqlark import Insert

# Insert a single record
result = Insert("posts")\
    .values([{"title": "New Post", "author_id": 1, "body": "Content here"}])\
    .execute(config)

# Insert with conflict handling (upsert)
result = Insert("posts")\
    .values([{"id": 1, "title": "Updated Post", "author_id": 1}])\
    .on_conflict('id', 'update')\
    .execute(config)
```

#### Update Data

```python
from sqlark import Update

result = Update("posts")\
    .set({"title": "Updated Title", "updated_at": "2024-01-15"})\
    .where(column="id", operator="=", value=1)\
    .execute(config)
```

#### Delete Data

```python
from sqlark import Delete

result = Delete("posts")\
    .where(column="id", operator="=", value=1)\
    .execute(config)
```

## Understanding Response Formatters

Response formatters are powerful tools that transform raw database results into more useful formats. SQLark provides several built-in formatters, each designed for different use cases.

### Default Response Formatter (List of Dictionaries)

By default, queries return a list of dictionaries with column names prefixed by table names:

```python
result = Select("posts").execute(config)
# [
#     {'posts.id': 1, 'posts.title': 'First Post', 'posts.author_id': 1},
#     {'posts.id': 2, 'posts.title': 'Second Post', 'posts.author_id': 2}
# ]
```

### Decomposed Dictionary Response Formatter

The `respond_with_decomposed_dict()` formatter organizes columns by table name, making it easier to work with joins:

```python
result = Select("posts")\
    .join(right_table="authors", left_col="author_id", right_col="id")\
    .respond_with_decomposed_dict()\
    .execute(config)

# [
#     {
#         'posts': {'id': 1, 'title': 'First Post', 'author_id': 1},
#         'authors': {'id': 1, 'name': 'John Doe', 'email': 'john@example.com'}
#     },
#     ...
# ]
```

This is especially useful when:
- You need to distinguish between columns from different tables
- You're working with joins and want organized access to each table's data
- You want to process data from each table separately

### Object Response Formatter

The `respond_with_object()` formatter creates Python objects from query results, providing attribute-based access to data:

#### Single Table Query

When querying a single table, you get a list of objects with attributes for each column:

```python
result = Select("posts")\
    .respond_with_object()\
    .execute(config)

# Access data using attributes
print(result[0].id)        # 1
print(result[0].title)     # "First Post"
print(result[0].author_id) # 1
```

#### Query with JOINs

When querying multiple tables with joins, each result is a `Row` object containing nested objects for each table:

```python
result = Select("posts")\
    .join(right_table="authors", left_col="author_id", right_col="id")\
    .respond_with_object()\
    .execute(config)

# Access nested objects
print(result[0].posts.title)      # "First Post"
print(result[0].authors.name)     # "John Doe"
print(result[0].authors.email)    # "john@example.com"
```

Benefits of object response formatter:
- **Type safety**: Access data using attributes instead of dictionary keys
- **IDE support**: Get autocomplete and type hints
- **Cleaner code**: More readable and Pythonic
- **Nested structure**: Automatically organizes joined table data

### RelationFormatter - Creating Hierarchical Object Structures

The `RelationFormatter` is the most powerful formatter, enabling you to create hierarchical, nested object structures that represent real-world relationships like "a post has many comments" or "a comment belongs to one author."

#### Basic One-to-Many Relationship

Create a hierarchy where posts contain a list of their comments:

```python
from sqlark.response_formatters import RelationFormatter

# Define the relationship
formatter = RelationFormatter()\
    .set_relation("posts.comments", "comments", relationship_type="many")

result = Select("posts")\
    .join(right_table="comments", left_col="id", right_col="post_id")\
    .respond_with_associated_objects(formatter)\
    .execute(config)

# Access nested comments
print(result[0].title)                    # "First Post"
print(len(result[0].comments))            # 3
print(result[0].comments[0].text)         # "Great post!"
print(result[0].comments[1].text)         # "Thanks for sharing"
```

In this example:
- `result` is a list of post objects
- Each post has a `comments` attribute containing a list of comment objects
- Multiple database rows (one per comment) are collapsed into a single post object with nested comments

#### One-to-One Relationship

Create a hierarchy where each comment belongs to one author:

```python
formatter = RelationFormatter()\
    .set_relation("comments.author", "authors", relationship_type="one")

result = Select("comments")\
    .join(right_table="authors", left_col="author_id", right_col="id")\
    .respond_with_associated_objects(formatter)\
    .execute(config)

# Access the author object
print(result[0].text)           # "Great post!"
print(result[0].author.name)    # "Jane Smith"
print(result[0].author.email)   # "jane@example.com"
```

#### Multi-Level Nested Relationships

Combine multiple relationships to create complex hierarchical structures:

```python
# Posts have many comments, and each comment has one author
formatter = RelationFormatter()\
    .set_relation("posts.comments", "comments", relationship_type="many")\
    .set_relation("comments.author", "authors", relationship_type="one")

result = Select("posts")\
    .join(right_table="comments", left_col="id", right_col="post_id")\
    .join(
        left_table="comments",
        right_table="authors",
        left_col="author_id",
        right_col="id"
    )\
    .respond_with_associated_objects(formatter)\
    .execute(config)

# Navigate the hierarchy
for post in result:
    print(f"Post: {post.title}")
    for comment in post.comments:
        print(f"  Comment by {comment.author.name}: {comment.text}")

# Output:
# Post: First Post
#   Comment by Jane Smith: Great post!
#   Comment by Bob Jones: Thanks for sharing
```

#### How RelationFormatter Works

The `RelationFormatter` transforms flat database results into nested structures:

1. **Define relationships** using `set_relation(attribute_name, foreign_table, relationship_type)`
   - `attribute_name`: Format is `"table.attribute"` (e.g., `"posts.comments"`)
   - `foreign_table`: The table containing the related data
   - `relationship_type`: Either `"one"` (single object) or `"many"` (list of objects)

2. **Automatic deduplication**: The formatter automatically handles duplicate parent records:
   ```python
   # Database returns:
   # posts.id | posts.title | comments.id | comments.text
   # 1        | First Post  | 1           | Comment 1
   # 1        | First Post  | 2           | Comment 2
   # 1        | First Post  | 3           | Comment 3
   
   # Result is ONE post object with THREE comment objects
   ```

3. **Null handling**: When a join returns NULL for a related table (e.g., a post with no comments), the relationship attribute is set appropriately:
   - For `relationship_type="many"`: An empty list `[]`
   - For `relationship_type="one"`: `None`

#### Real-World Example: Blog with Nested Comments

```python
from sqlark import Select, PostgresConfig
from sqlark.response_formatters import RelationFormatter

# Configure database connection (use environment variables in production)
config = PostgresConfig(dbname="blog", user="postgres", password="your_password")

# Create formatter with multi-level relationships
formatter = RelationFormatter()\
    .set_relation("posts.comments", "comments", relationship_type="many")\
    .set_relation("posts.author", "authors", relationship_type="one")\
    .set_relation("comments.author", "authors", relationship_type="one")

# Build query with multiple joins
query = Select("posts")\
    .join(right_table="comments", left_col="id", right_col="post_id")\
    .join(
        left_table="posts",
        right_table="authors",
        left_col="author_id",
        right_col="id"
    )\
    .join(
        left_table="comments",
        right_table="authors",
        left_col="author_id",
        right_col="id"
    )\
    .where(table="posts", column="published", operator="=", value=True)\
    .order_by("created_at", table="posts", direction="DESC")\
    .limit(10)

# Execute with formatter
posts = query.respond_with_associated_objects(formatter).execute(config)

# Use the hierarchical data
for post in posts:
    print(f"\n{post.title} by {post.author.name}")
    print(f"Published: {post.created_at}")
    print(f"\nComments ({len(post.comments)}):")
    for comment in post.comments:
        print(f"  - {comment.author.name}: {comment.text}")
```

### Choosing the Right Response Formatter

| Formatter | Use When | Returns |
|-----------|----------|---------|
| **Default** | Simple queries, raw data processing | `[{'table.col': val}, ...]` |
| **Decomposed Dict** | Working with joins, need organized table data | `[{'table': {'col': val}}, ...]` |
| **Object** | Want attribute access, type safety, single-level data | Objects with attributes |
| **RelationFormatter** | Need nested/hierarchical structures, parent-child relationships | Nested objects with related data |

## Advanced Examples

### Complex Query with Multiple Joins and Conditions

```python
# Find all published posts by a specific author with their comments and tags
from sqlark import Select
from sqlark.response_formatters import RelationFormatter

formatter = RelationFormatter()\
    .set_relation("posts.comments", "comments", relationship_type="many")\
    .set_relation("posts.tags", "tags", relationship_type="many")

query = Select("posts")\
    .join(right_table="authors", left_col="author_id", right_col="id")\
    .join(right_table="comments", left_col="id", right_col="post_id")\
    .join(
        left_table="posts",
        right_table="post_tags",
        left_col="id",
        right_col="post_id"
    )\
    .join(
        left_table="post_tags",
        right_table="tags",
        left_col="tag_id",
        right_col="id"
    )\
    .where(table="authors", column="name", operator="=", value="John Doe")\
    .where_and(table="posts", column="published", operator="=", value=True)\
    .where_and(table="posts", column="created_at", operator=">", value="2024-01-01")\
    .order_by("created_at", table="posts", direction="DESC")\
    .limit(20)

results = query.respond_with_associated_objects(formatter).execute(config)
```

### Counting and Grouping

```python
from sqlark import Count

# Count posts by author
result = Count("posts")\
    .join(right_table="authors", left_col="author_id", right_col="id")\
    .group_by("name", table="authors")\
    .execute(config)

# With object formatter for cleaner access
result = Count("posts")\
    .join(right_table="authors", left_col="author_id", right_col="id")\
    .group_by("name", table="authors")\
    .respond_with_object()\
    .execute(config)

for item in result:
    print(f"{item.name}: {item.count} posts")
```

### Using WHERE IN for Multiple Values

```python
author_ids = [1, 2, 3, 4, 5]

result = Select("posts")\
    .where_in(column="author_id", values=author_ids)\
    .execute(config)
```

### Transactional Operations

```python
# Execute multiple operations in a transaction
from sqlark import Insert, Update

# All operations succeed or all fail together
try:
    # Insert a new post
    Insert("posts")\
        .values([{"title": "New Post", "author_id": 1}])\
        .execute(config, transactional=True)
    
    # Update author's post count
    Update("authors")\
        .set({"post_count": "post_count + 1"})\
        .where(column="id", operator="=", value=1)\
        .execute(config, transactional=True)
except Exception as e:
    print(f"Transaction failed: {e}")
```

## Development
```sh
pip3 install --upgrade pip poetry
poetry self add "poetry-dynamic-versioning[plugin]"
poetry install
```

## Unit Testing

Start a postgres database. The tests don't modify the database, but do require an active psycopg2 connection.

```
docker run -d -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
```

Run the unit tests
```
poetry run pytest
```
You may need to set PGUSER, PGDATABASE, and PGPASSWORD environment variables to establish the connection

Copyright 2024 - Meadowlark Engineering