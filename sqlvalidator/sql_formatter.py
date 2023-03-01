from sqlvalidator.grammar.lexer import SQLStatementParser
from sqlvalidator.grammar.tokeniser import to_tokens


def format_sql(sql_string: str) -> str:
    """Tokenize, parse, and format the input SQL query string."""
    return SQLStatementParser.parse(to_tokens(sql_string)).transform()
