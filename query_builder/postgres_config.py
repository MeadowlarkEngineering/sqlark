"""
Postgres Configuration
"""
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import DictCursor

class PostgresConfig:
    """Configurations required for the Postgres client."""

    def __init__(self,
                 dbname: str = None,
                 user: str = None,
                 password: str = None,
                 host: str = None,
                 port: str = None,
                 dsn: str = None,
                 ):
        """Configuration values for the Postgres client."""
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dsn = dsn

    @property
    def connection_params(self):
        """Parameters for connecting to postgres database"""
        if self.dsn is not None:
            return {'dsn': self.dsn}

        params = {}
        if self.dbname is not None:
            params['dbname'] = self.dbname
        if self.user is not None:
            params['user'] = self.user
        if self.password is not None:
            params['password'] = self.password
        if self.host is not None:
            params['host'] = self.host
        if self.port is not None:
            params['port'] = self.port
        return params


    @contextmanager
    def connect_with_cursor(self, transactional=False):
        """Connect to database"""
        with psycopg2.connect(**self.connection_params) as connection:
            if not transactional:
                connection.set_session(autocommit=True)

            with connection.cursor(cursor_factory=DictCursor) as cursor:
                yield cursor
