"""
Unit testing for Insert SQLCommand
"""
from unittest import mock
from query_builder import Insert, PostgresConfig
import pytest

def test_insert_01(pg_connection):
    """
    tests simple initialization
    """
    
    s = Insert(table_name="comments").values([{"author":"Clark Kent", "body":"Up and away!"}])
    assert s.to_sql(PostgresConfig()).as_string(pg_connection).strip() == 'INSERT INTO "comments" ("author","body") VALUES %s  RETURNING *'


def test_insert_02(pg_connection):
    """
    tests insert with on conflict
    """
    
    s = Insert(table_name="comments").values([{"author":"Clark Kent", "body":"Up and away!"}]).on_conflict("author", "nothing")
    assert s.to_sql(PostgresConfig()).as_string(pg_connection).strip() == 'INSERT INTO "comments" ("author","body") VALUES %s ON CONFLICT ("author") DO NOTHING RETURNING *'

def test_insert_03(pg_connection):
    """
    tests insert with on conflict and update
    """
    
    s = Insert(table_name="comments").values([{"author":"Clark Kent", "body":"Up and away!"}]).on_conflict("author", "update")
    assert s.to_sql(PostgresConfig()).as_string(pg_connection).strip() == 'INSERT INTO "comments" ("author","body") VALUES %s ON CONFLICT ("author") DO UPDATE SET "author" = COALESCE(EXCLUDED."author", "author"),"body" = COALESCE(EXCLUDED."body", "body") RETURNING *'
