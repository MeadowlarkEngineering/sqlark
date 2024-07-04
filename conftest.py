import pytest
import psycopg2

@pytest.fixture
def pg_connection():
    return psycopg2.connect()