from typing import Generator, Iterable, Iterator, List, Optional, Tuple

STRING_SPLIT_TOKENS = ("'", '"', "`")
WHITESPACE_SPLIT_TOKENS = (" ", "\n", "\t")
KEPT_SPLIT_TOKENS = (
    ",",
    ";",
    "(",
    ")",
    "[",
    "]",
    "+",
    "-",
    "*",
    "/",
    "=",
    "<",
    ">",
    ".",
)
MERGE_TOKENS = ("<>", "<=", ">=", "<<", ">>", "||", "!=")


def lower(s: Optional[str]) -> Optional[str]:
    """Return lowercase representation of a string if it is not None."""
    return s.lower() if s else s


def get_tokens_until_closing_parenthesis(
    tokens: Iterator[str], first_token: Optional[str] = None
) -> List[str]:
    """Consume tokens from iterator until a matching closing parenthesis is found."""
    argument_tokens = []
    next_token = first_token or next(tokens, None)
    count_parenthesis = 0
    while next_token is not None and not (next_token == ")" and count_parenthesis == 0):
        argument_tokens.append(next_token)
        if next_token == "(":
            count_parenthesis += 1
        elif next_token == ")":
            count_parenthesis -= 1
        next_token = next(tokens, None)

    return argument_tokens


def get_tokens_until_one_of(
    tokens: Iterator[str],
    stop_words: Iterable[str],
    first_token: Optional[str] = None,
    keep: Optional[Iterable[Tuple[str, str]]] = None,
) -> Tuple[List[str], Optional[str]]:
    """Consume tokens until a stop word is encountered at the top level of nesting."""
    argument_tokens = [first_token] if first_token is not None else []
    keep_pairs = keep or []

    next_token = next(tokens, None)
    count_parenthesis = 0 if first_token != "(" else 1
    count_square_brackets = 0 if first_token != "[" else 1
    count_case_expr = 0 if first_token != "case" else 1
    while next_token is not None and not (
        lower(next_token) in stop_words
        and count_parenthesis <= 0
        and count_square_brackets <= 0
        and count_case_expr <= 0
        and (
            not argument_tokens
            or (lower(argument_tokens[-1]), lower(next_token)) not in keep_pairs
        )
    ):
        argument_tokens.append(next_token)
        if next_token == "(":
            count_parenthesis += 1
        elif next_token == ")":
            count_parenthesis -= 1
        elif next_token == "[":
            count_square_brackets += 1
        elif next_token == "]":
            count_square_brackets -= 1
        elif lower(next_token) == "case":
            count_case_expr += 1
        elif lower(next_token) == "end":
            count_case_expr -= 1
        next_token = next(tokens, None)

    return argument_tokens, next_token


def get_tokens_until_not_in(
    tokens: Iterator[str], kept_words: Iterable[str], first_token: Optional[str] = None
) -> Tuple[List[str], Optional[str]]:
    """Consume tokens as long as they are part of the kept_words collection."""
    argument_tokens = [first_token] if first_token is not None else []
    next_token = next(tokens, None)
    while next_token is not None and lower(next_token) in kept_words:
        argument_tokens.append(next_token)
        next_token = next(tokens, None)

    return argument_tokens, next_token


def split_with_sep(s: str, sep: str) -> Generator[str, None, None]:
    """Split string by separator, keeping the separator in the yielded output."""
    splitted = split_with_escaping(s, sep)
    for word in splitted[:-1]:
        if word:
            yield word
        yield sep
    if splitted[-1]:
        yield splitted[-1]


def split_with_escaping(s: str, sep: str) -> List[str]:
    """Split string by separator, respecting backslash escapes."""
    splitted = []
    split_iterator = iter(s.split(sep))
    value = next(split_iterator, None)
    while value is not None:
        # Glue values together if backslash-escaped
        if value.endswith("\\"):
            value += sep
            next_value = next(split_iterator, None)
            if next_value:
                value += next_value
        # Add to list, and keep looping
        splitted.append(value)
        value = next(split_iterator, None)
    return splitted


def merge_stream(
    s: Iterator[str], goals: Iterable[str]
) -> Generator[str, None, None]:
    """Merge tokens in the stream if they form one of the composite goal tokens."""
    for element in s:
        matching_goals = [g for g in goals if g.startswith(element)]
        if not matching_goals:
            yield element
            continue
        next_element = next(s, None)
        if not next_element:
            yield element
            continue

        twice_matching_goals = [
            g for g in matching_goals if (element + next_element) == g
        ]
        if len(twice_matching_goals) > 1:
            raise ValueError("Should not reach here")
        elif twice_matching_goals:
            yield twice_matching_goals[0]
        else:
            yield element
            yield next_element


def _split_on_string_token(token: str, value: str) -> Generator[str, None, None]:
    """Recursively split value around string quotes, preserving quotes and string literal content."""
    split_result = split_with_sep(value, token)
    in_str = False
    for elem in split_result:
        if elem == token:
            in_str = not in_str
            yield elem
        elif in_str is True:
            yield elem
        else:
            yield from split_tokens(elem)


def _split_on_whitespace_token(token: str, value: str) -> Generator[str, None, None]:
    """Split value on whitespace separator, recursively tokenizing the components."""
    for elem in value.split(token):
        if elem:
            yield from split_tokens(elem)


def _split_on_kept_token(token: str, value: str) -> Generator[str, None, None]:
    """Split value on kept single character operators, except for decimals in numbers."""
    if token == "." and "." in value and value.replace(".", "").isdigit():
        yield value
    else:
        for elem in split_with_sep(value, token):
            yield from split_tokens(elem)


def split_tokens(value: str) -> Generator[str, None, None]:
    """Tokenize a string by strings, whitespaces, and punctuation/operators recursively."""
    for string_token in sorted(
        STRING_SPLIT_TOKENS,
        key=lambda token: value.index(token) if token in value else (len(value) + 1),
    ):
        if string_token in value:
            yield from _split_on_string_token(string_token, value)
            return

    for whitespace_token in WHITESPACE_SPLIT_TOKENS:
        if whitespace_token in value:
            yield from _split_on_whitespace_token(whitespace_token, value)
            return

    for kept_token in KEPT_SPLIT_TOKENS:
        if kept_token in value and value != kept_token:
            yield from _split_on_kept_token(kept_token, value)
            return

    yield value


def strip_sql_comments(sql: str) -> str:
    """Remove single-line (--) and block (/* ... */) comments from a SQL string while preserving quotes."""
    out = []
    chars = list(sql)
    n = len(chars)
    i = 0
    in_string = None  # Can be "'", '"', or "`"

    while i < n:
        if in_string:
            out.append(chars[i])
            if chars[i] == '\\' and i + 1 < n:
                out.append(chars[i+1])
                i += 2
                continue
            if chars[i] == in_string:
                in_string = None
            i += 1
            continue

        if chars[i] in ("'", '"', "`"):
            in_string = chars[i]
            out.append(chars[i])
            i += 1
            continue

        if i + 1 < n and chars[i] == '-' and chars[i+1] == '-':
            i += 2
            while i < n and chars[i] != '\n':
                i += 1
            continue

        if i + 1 < n and chars[i] == '/' and chars[i+1] == '*':
            i += 2
            while i + 1 < n and not (chars[i] == '*' and chars[i+1] == '/'):
                i += 1
            i += 2
            continue

        out.append(chars[i])
        i += 1

    return "".join(out)


def to_tokens(value: str) -> Generator[str, None, None]:
    """Clean the SQL string and convert it into a stream of tokens."""
    cleaned_value = strip_sql_comments(value)
    tokens = split_tokens(cleaned_value)
    yield from merge_stream(tokens, MERGE_TOKENS)
