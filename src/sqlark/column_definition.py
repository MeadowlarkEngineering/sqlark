"""
Module for a column definition
"""

from dataclasses import dataclass
from psycopg2 import sql


@dataclass
class ColumnDefinition:
    """
    Represents a column definition
    """

    # pylint: disable=too-many-instance-attributes
    table_name: str
    name: str
    data_type: str
    is_nullable: bool = True
    default: str | None = None
    is_list: bool = False
    alias: str | None = None
    function: str | None = None

    def __post_init__(self):
        """
        Post-initialization to set default alias if not provided
        """
        if self.alias is None:
            self.alias = f"{self.table_name}.{self.name}"

    def column_name_from_alias(self) -> str:
        """
        Extract the column name from the alias
        If the alias is in the format "table_name.column_name", return "column_name"
        Otherwise, return the alias as is
        """
        if self.alias and "." in self.alias:
            return self.alias.split(".")[1]
        if self.alias:
            return self.alias
        return self.name

    def format_with_alias(self) -> sql.Composed:
        """
        Format the column definition with alias.
        If no alias was provided, use table_name.column_name as the alias.

        :param self: Description
        :return: Description
        :rtype: Composed
        """

        if not self.alias:
            raise ValueError("Alias must be set for format_with_alias")

        if self.function:
            return sql.SQL("{} {}").format(
                sql.SQL(self.function),
                sql.Identifier(self.alias),
            )

        return sql.SQL("{}.{} as {}").format(
            sql.Identifier(self.table_name),
            sql.Identifier(self.name),
            sql.Identifier(self.alias),
        )

    def format_without_alias(self) -> sql.Composed:
        """
        Format the column definition without alias

        :param self: Description
        :return: Description
        :rtype: Composed
        """
        if self.function:
            return sql.SQL("{}").format(
                sql.SQL(self.function),
            )

        return sql.SQL("{}.{}").format(
            sql.Identifier(self.table_name),
            sql.Identifier(self.name),
        )
