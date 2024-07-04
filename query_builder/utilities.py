"""
Useful standalone mixins
"""

import logging
from psycopg2 import sql
from query_builder.postgres_config import PostgresConfig

TABLE_COLUMN_CACHE = {}


logging.basicConfig(level=logging.DEBUG)


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
    if use_cache and table_name in TABLE_COLUMN_CACHE:
        return TABLE_COLUMN_CACHE[table_name]

    try:
        command = sql.SQL(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name=%s
            """
        )
        params = [table_name]
        with pg_config.connect_with_cursor() as cursor:
            cursor.execute(command, params)
            result = cursor.fetchall()

        columns = [v["column_name"] for v in result]
        TABLE_COLUMN_CACHE[table_name] = columns
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
