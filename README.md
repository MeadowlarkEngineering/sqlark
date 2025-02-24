# SQLark (Sequel-lark)

A lightweight Non-ORM for postgresql.

## What is a Non-ORM?

A Non-ORM provides the benefits of an Object-Relational-Mapping without the overhead associated with creating a detailed map to the database schema in the application.

Popular ORM libraries require the application developer to define the database to object mapping with specialized classes.
Some ORMs even need to manage and generate the database schema in order to properly work.  These heavy-weight ORMs treat
the database as a second class citizen, a mere  data storage servant.

There are significant advantages to managing a database schema separately from the application, and these advantages are lost when control is handed over to the ORM.  Native database views, triggers, and procedures can enhance reliability, itegrity, and performance of an application.
Not to mention they create a separation between application development and data management, which has undeniable security advantages.

SQLark does not require any a-priori knowledge or mapping of the database schema to objects.  The database is inspected at runtime and objects are dynamically generated that match the schema.  This approach eliminates an enormous amount of cumbersome and unnecessary code, while keeping the advantages of interacting with database results as objects.

## How does it work?

SQLark takes a very simple approach to generating objects from database queries.  When SQLark generates a query it "composes" new column names for each column in the results set.  For example, imagine a table `muppets` with columns `id`, `name`.

The SQLark operator `Select("muppets").execute(config)` will create a query to select data from this table as follows:

```sql
SELECT "muppets"."id" as "muppets.id", "muppets"."name" as "muppets.name", from "muppets"
```

And the result set will be:
```
|  muppets.id  |  muppets.name  |
---------------------------------
|  1          |  Bert         |
|  2          |  Ernie        |
|  3          |  Elmo         |
```

This act of renaming the columns as "table.column" means that each row of the result set can be decomposed using string operations into a dictionary keyed by the table name.
For example, calling to decompose the result set above use `respond_with_decomposed_dict() as follows`.

```python
Select("muppets").respond_with_decomposed_dict().execute(config)
---
[
    {"muppets": {"id": 1, "name": "Bert"}},
    {"muppets": {"id": 2, "name": "Ernie"}},
    {"muppets": {"id": 3, "name": "Elmo"}},
]
```

Notice that each row has been partitioned and formatted as a dictionary such that the table name becomes a key whose value is another dictionary with attributes for each column.

Finally, using `respond_with_object()` instructs SQLark to dynamically construct a dataclass, and then use the decomposed dictionaries to instantiate dataclass objects.  Formatting the decomposed result set above as an object  yields:

```python
Select("muppets").respond_with_object().execute(config)
---
[
    Muppets(id=1, name="Bert"),
    Muppets(id=2, name="Ernie"),
    Muppets(id=3, name="Elmo")
]
```

The result is a list of objects whose attributes are the columns in the database.  All this is done automatically without any need to define a `Muppets` class or instantiate `Muppets` objects in the application code.


## But what about joins?

The single table example is trivial, and doesn't offer much advantage over traditional ORMs or even direct database queries.  The power of the SQLark comes when joining related tables.

When joining, there is a dictionary key for each table in the join for each row.

```python
Select("muppets")
    .join(right_table="pets", left_col="id", right_col="muppet_id")
    .respond_with_decomposed_dict()
    .execute(config)
---

[
    {
        "muppets": {"id": 1, "name": "Bert"},
        "pets": {"id": 1, "name": "Bernice", "species": "pigeon", "muppet_id": 1}
    },
    {
        "muppets": {"id": 2, "name": "Ernie"},
        "pets": {"id": 2, "name": "Rubbery Duckie", "species": "Rubber Ducky", "muppet_id": 2}
    },
    {
        "muppets": {"id": 2, "name": "Ernie"},
        "pets": {"id": 4, "name": "Rufus", "species": "Firefly", "muppet_id": 2}
    },
    {
        "muppets": {"id": 3, "name": "Elmo"},
        "pets": {"id": 3, "name": "Dorothy", "species": "Goldfish", "muppet_id": 3}
    }
]
```

Using `respond_with_object()` will return a similar list with objects instead of dictionaries.  Each row now represents multiple objects, so an artifical Row object is created with an attribute for each table represented in the result set.


```python
Select("muppets")
    .join(right_table="pets", left_col="id", right_col="muppet_id")
    .respond_with_object()
    .execute(config)
---

[
    Row(
        muppets=People(id=1, name="Bert"),
        pets=Pets(id=1, name="Bernice", species="pigeon", muppet_id=1)
    ),
    Row(
        muppets=People(id=2, name="Ernie"),
        pets=Pets(id=2, name="Rubbery Duckie", species="Rubber Ducky", muppet_id=2)
    ),
    Row(
        muppets=People(id=2, name="Ernie"),
        pets=Pets(id=4, name="Rufus", species="Firefly", muppet_id=2)
    ),
    Row(
        muppets=People(id=3, name="Elmo"),
        pets=Pets(id=3, name="Dorothy", species="Goldfish", muppet_id=3)
    )
]
```

This, however, is still not entirely useful.  In this example it would be most useful if each muppet had an attribute `pets` which was a list of their pets.  We use a `RelationFormatter` to accomplish this.

```python

Select("muppets")
    .join(right_table="pets", left_col="id", right_col="muppet_id")
    .respond_with_associated_objects(
        RelationFormatters()
        .set_relation(
            attribute_name="muppets.pets",
            foreign_table="pets",
            relationship_type="many"
        )
    )
    .execute(config)
---
[
    Muppets(id=1, name="Bert", pets=[
        Pets(id=1, name="Bernice", species="pigeon")
    ]),
    Muppets(id=2, name="Ernie", pets=[
        Pets(id=2, name="Rubber Duckie", species="Rubber Ducky"),
        Pets(id=4, name="Rufus", species="firefly"),
    ]),
    Muppets(id=3, name="Elmo", pets=[
        Pets(id=3, name="Dorothy", species="goldfish")
    ])
]
```

A RelationFormatter has one or more relations, where each relation is specified as the attribute and the foreign table to inject into that attribute. In the example above the attribute is `muppets.pets` and the foreign table is `pets`.  The attribute must always be specified as `<table name>.<attribute name>`.  The formatter now know inspects each row and adds a new list attribute `pets` to the `Muppets` object, inserting each `Pets` object into the new attribute.

## Getting Started

### Installation

Using pip
```sh
pip install git+https://github.com/MeadowlarkEngineering/query-builder.git#egg=query-builder
```

Using poetry
```sh
poetry add git+https://github.com/MeadowlarkEngineering/query-builder.git
```

### Usage

#### Select

Select everything from a table.

```python
from query_builder import PostgresConfig, Select
config = PostgresConfig(dbname="blog", user="postgres-username", password="postgres-password)
Select("comments").execute(config)
> [{'id': 1, 'text': "A comment"}]
```

Add joins, multiple where clauses, offsets, and limits.

```python
s = Select(table_name="posts").\
    join(left_table="posts", right_table="authors", left_col="author_id", right_col="id").\
    where(table="authors", column="id", operator="=", value=1).\
    where_and(table="authors", column="created_at", operator=">", value="2024-01-01").\
    where_and(table="authors", column="created_at", operator=">", value="2024-01-01").\
    limit(10).\
    offset(5).\
    order_by("created_at", table="posts")
result = s.execute(config)
```

Insert data
```python
i = Insert(table_name="posts").values([{"author_id": 1, "body": "this is a post"}]).on_conflict('id', 'update')
result = i.execute(config)
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
