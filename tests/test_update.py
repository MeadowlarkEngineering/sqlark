"""
Unit testing for Select SQLCommand
"""
from unittest import mock
from query_builder import Update, Where, PostgresConfig
import pytest

def test_update_01(pg_connection):
    """
    tests simple initialization
    """
    
    s = Update(table_name="comments").set(values={"author": "John Doe", "body": "Hello World"}).where(column="id", operator="=", value=1)
    assert s.to_sql(PostgresConfig()).as_string(pg_connection).strip() == \
        'UPDATE "comments" SET "author"=%s,"body"=%s  WHERE "comments"."id" = %s RETURNING *'
    assert s.get_params() == ["John Doe", "Hello World", 1]
