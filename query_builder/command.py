"""
Abstract SQL command class
"""

from abc import ABC, abstractmethod
import psycopg2
from query_builder.postgres_config import PostgresConfig
from query_builder.utilities import get_logger


class SQLCommand(ABC):
    """
    Abstract SQL command class.  Subclasses must implement the to_sql and execute methods
    """

    def __init__(self):
        self.logger = get_logger(__name__)

    @abstractmethod
    def to_sql(self, pg_config: PostgresConfig) -> psycopg2.sql.SQL:
        """
        Returns the SQL representation of the command
        """
        raise NotImplementedError

    @abstractmethod
    def execute(self, pg_config: PostgresConfig):
        """
        Executes the command
        """
        raise NotImplementedError
