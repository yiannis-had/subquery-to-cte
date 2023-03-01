from typing import Any, List, Optional

from sqlvalidator.grammar.lexer import ParsingError, SQLStatementParser
from sqlvalidator.grammar.sql import Expression
from sqlvalidator.grammar.tokeniser import to_tokens


class SQLQuery:
    """Represents a SQL query that can be formatted and validated."""

    def __init__(self, sql: str) -> None:
        self.sql: str = sql
        self._sql_query: Optional[Expression] = None
        self.validated: bool = False
        self.errors: List[str] = []

    @property
    def sql_query(self) -> Expression:
        """Parse the SQL and return its AST query object representation."""
        if self._sql_query is None:
            self._sql_query = SQLStatementParser.parse(to_tokens(self.sql))
        return self._sql_query

    def format(self) -> str:
        """Return the formatted SQL string representation of this query."""
        return self.sql_query.transform()

    def is_valid(self) -> bool:
        """Validate the query syntax and schema constraints and return True if valid."""
        if not self.validated:
            self._validate()
        return len(self.errors) == 0

    def _validate(self) -> None:
        """Internal validation helper to invoke parsing and AST validation."""
        self.validated = True
        try:
            self.errors = self.sql_query.validate()
        except ParsingError as ex:
            self.errors.append(str(ex))


def parse(sql: str) -> SQLQuery:
    """Parse a SQL string and return an SQLQuery representation."""
    query = SQLQuery(sql)
    return query
