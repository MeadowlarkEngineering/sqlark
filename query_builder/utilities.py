"""
Useful standalone mixins
"""
from collections import namedtuple
import logging
from psycopg2 import sql
from query_builder.postgres_config import PostgresConfig

TABLE_COLUMN_CACHE = {}


logging.basicConfig(level=logging.DEBUG)

ColumnDefinition = namedtuple("ColumnDefinition", ["name", "data_type", "is_nullable","default"])

def get_logger(name):
    """
    Returns a preconfigured logger
    """
    return logging.getLogger(name)


def get_columns(table_name, pg_config: PostgresConfig, use_cache=True) -> list[str]:
    """
    Retrieves the fields of the table entity_type.

    params:
        table_name: str The name of the table to retrieve the fields from
        use_cache: bool Return the cached value if available
    returns:
        list[str] The fields of the table
    raises:
        ValueError: If the fields could not be retrieved
    """
    columns = get_column_definitions(table_name, pg_config, use_cache=use_cache)
    return [c.name for c in columns]


def get_column_definitions(table_name, pg_config: PostgresConfig, use_cache=True) -> list[ColumnDefinition]:
    """
    Retrieves the column definitions of the table.
    """

    if use_cache and table_name in TABLE_COLUMN_CACHE:
        return TABLE_COLUMN_CACHE[table_name]    


    try:
        command = sql.SQL(
            """
            SELECT column_name name, data_type, is_nullable, column_default default
            FROM information_schema.columns 
            WHERE table_name=%s
            """
        )
        params = [table_name]
        with pg_config.connect_with_cursor() as cursor:
            cursor.execute(command, params)
            result = cursor.fetchall()

        columns = [ColumnDefinition(**v) for v in result]
        TABLE_COLUMN_CACHE[table_name] = columns

        if len(columns) == 0:
            raise ValueError(f"Table {table_name} has no columns")

        return columns

    except Exception as e:
        raise ValueError(
            f"Could not retrieve the fields for {table_name} - {str(e)}"
        ) from e

def get_columns_composed(
    table_name, pg_config: PostgresConfig, use_cache=True
) -> sql.Composed:
    """
    Retrieves the table columns as a Composed
    where each column is aliased as "table_name"."column_name"
    params:
        table_name: str The name of the table
    returns:
        sql.Composed The columns of the table as a composed SQL object
    raises:
        ValueError: If the columns could not be retrieved
    """
    format_method = lambda x: sql.SQL("{}.{} as {}").format(
        sql.Identifier(table_name), sql.Identifier(x),
        sql.Identifier(f"{table_name}.{x}")
    )

    return sql.Composed(
        [
            format_method(c)
            for c in get_columns(table_name, pg_config, use_cache=use_cache)
        ]
    )


def data_type_to_field_type(data_type: str) -> type:
    """
    Converts a postgres data type to a python data type
    """
    data_type_map = {
        "character varying": str,
        "text": str,
        "integer": int,
        "double precision": float,
        "timestamp without time zone": datetime,
    }
    return data_type_map.get(data_type, str)
