import sqlvalidator
from sqlvalidator.grammar.sql import (
    SelectStatement,
    WithStatement,
    Table,
    Alias,
    Parenthesis,
    Join,
    transform,
    Column,
    WithQuery,
    CombinedQueries,
    String,
    Integer,
    Float,
    Null,
    Boolean,
)

class TableCollector:
    def __init__(self):
        self.tables = set()

    def collect(self, node) -> None:
        if node is None:
            return
        if isinstance(node, Table):
            self.tables.add(str(node.value).lower())
            return
        if isinstance(node, (str, int, float, bool)):
            return
        if isinstance(node, list):
            for item in node:
                self.collect(item)
            return
        if isinstance(node, tuple):
            for item in node:
                self.collect(item)
            return
        if isinstance(node, dict):
            for v in node.values():
                self.collect(v)
            return
        if hasattr(node, '__dict__'):
            for v in node.__dict__.values():
                self.collect(v)

class CTERewriter:
    def __init__(self):
        self.ctes = []
        self.used_names = set()
        self.counter = 0

    def get_cte_name(self, alias=None) -> str:
        if alias:
            alias_str = str(alias).strip('`"\'').lower()
            # Clean non-alphanumeric characters
            alias_str = "".join(c if (c.isalnum() or c == '_') else '_' for c in alias_str)
            if alias_str:
                candidate = alias_str
                if candidate not in self.used_names:
                    self.used_names.add(candidate)
                    return candidate
                i = 1
                while f"{candidate}_{i}" in self.used_names:
                    i += 1
                unique_name = f"{candidate}_{i}"
                self.used_names.add(unique_name)
                return unique_name

        self.counter += 1
        while f"cte_{self.counter}" in self.used_names:
            self.counter += 1
        name = f"cte_{self.counter}"
        self.used_names.add(name)
        return name

    def rewrite(self, root_node):
        collector = TableCollector()
        collector.collect(root_node)
        self.used_names.update(collector.tables)
        return self.rewrite_ast(root_node, is_from=False, is_root=True)

    def rewrite_ast(self, node, is_from=False, is_root=False, alias_hint=None):
        if node is None:
            return None

        # Terminal/constant types
        if isinstance(node, (Table, Column, String, Integer, Float, Null, Boolean, str, int, float, bool)):
            return node

        # Collections
        if isinstance(node, list):
            return [self.rewrite_ast(item, is_from, is_root=False) for item in node]
        if isinstance(node, tuple):
            return tuple(self.rewrite_ast(item, is_from, is_root=False) for item in node)
        if isinstance(node, dict):
            return {k: self.rewrite_ast(v, is_from, is_root=False) for k, v in node.items()}

        # SelectStatement
        if isinstance(node, SelectStatement):
            if is_root:
                node.expressions = self.rewrite_ast(node.expressions, is_from=False, is_root=False)
                node.from_statement = self.rewrite_ast(node.from_statement, is_from=True, is_root=False)
                node.where_clause = self.rewrite_ast(node.where_clause, is_from=False, is_root=False)
                node.group_by_clause = self.rewrite_ast(node.group_by_clause, is_from=False, is_root=False)
                node.having_clause = self.rewrite_ast(node.having_clause, is_from=False, is_root=False)
                node.order_by_clause = self.rewrite_ast(node.order_by_clause, is_from=False, is_root=False)
                node.limit_clause = self.rewrite_ast(node.limit_clause, is_from=False, is_root=False)
                node.offset_clause = self.rewrite_ast(node.offset_clause, is_from=False, is_root=False)
                return node
            else:
                # Subquery!
                node._original_sql = transform(node)
                # Rewrite children first (bottom-up)
                node.expressions = self.rewrite_ast(node.expressions, is_from=False, is_root=False)
                node.from_statement = self.rewrite_ast(node.from_statement, is_from=True, is_root=False)
                node.where_clause = self.rewrite_ast(node.where_clause, is_from=False, is_root=False)
                node.group_by_clause = self.rewrite_ast(node.group_by_clause, is_from=False, is_root=False)
                node.having_clause = self.rewrite_ast(node.having_clause, is_from=False, is_root=False)
                node.order_by_clause = self.rewrite_ast(node.order_by_clause, is_from=False, is_root=False)
                node.limit_clause = self.rewrite_ast(node.limit_clause, is_from=False, is_root=False)
                node.offset_clause = self.rewrite_ast(node.offset_clause, is_from=False, is_root=False)

                cte_name = self.get_cte_name(alias_hint)
                self.ctes.append((cte_name, node))

                if is_from:
                    return Table(cte_name)
                else:
                    return SelectStatement(
                        expressions=[Column('*')],
                        from_statement=Table(cte_name),
                        semi_colon=False
                    )

        if isinstance(node, CombinedQueries):
            node.left_query = self.rewrite_ast(node.left_query, is_from=is_from, is_root=is_root)
            node.right_query = self.rewrite_ast(node.right_query, is_from=is_from, is_root=is_root)
            return node

        if isinstance(node, WithStatement):
            node.with_queries = self.rewrite_ast(node.with_queries, is_from=False, is_root=False)
            node.select_statement = self.rewrite_ast(node.select_statement, is_from=False, is_root=is_root)
            return node

        if isinstance(node, Join):
            node.left_from = self.rewrite_ast(node.left_from, is_from=True, is_root=False)
            node.right_from = self.rewrite_ast(node.right_from, is_from=True, is_root=False)
            node.on = self.rewrite_ast(node.on, is_from=False, is_root=False)
            node.using = self.rewrite_ast(node.using, is_from=False, is_root=False)
            return node

        if isinstance(node, Alias):
            node.expression = self.rewrite_ast(node.expression, is_from=is_from, is_root=False, alias_hint=node.alias)
            # If the rewritten expression is a Table whose name is exactly the alias,
            # then the alias wrapper is redundant and can be discarded.
            if isinstance(node.expression, Table) and str(node.expression.value).lower() == str(node.alias).lower():
                return node.expression
            return node

        if isinstance(node, Parenthesis):
            new_args = tuple(self.rewrite_ast(arg, is_from=is_from, is_root=False, alias_hint=alias_hint) for arg in node.args)
            if len(new_args) == 1 and isinstance(new_args[0], Table):
                return new_args[0]
            node.args = new_args
            return node

        if hasattr(node, '__dict__'):
            for k, v in list(node.__dict__.items()):
                node.__dict__[k] = self.rewrite_ast(v, is_from=is_from, is_root=False)
            return node

        return node

class CommentedWithQuery(WithQuery):
    def __init__(self, name: str, statement: SelectStatement, comments: list):
        super().__init__(name, statement)
        self.comments = comments

    def __str__(self) -> str:
        comment_lines = []
        for c in self.comments:
            comment_lines.append(c.rstrip())
        if comment_lines:
            return "\n".join(comment_lines) + "\n" + super().__str__()
        return super().__str__()

def strip_comments_preserving_positions(sql: str) -> tuple[str, list[int]]:
    cleaned = []
    mapping = []
    chars = list(sql)
    n = len(chars)
    i = 0
    in_string = None
    
    while i < n:
        if in_string:
            cleaned.append(chars[i])
            mapping.append(i)
            if chars[i] == '\\' and i + 1 < n:
                cleaned.append(chars[i+1])
                mapping.append(i+1)
                i += 2
                continue
            if chars[i] == in_string:
                in_string = None
            i += 1
            continue
            
        if chars[i] in ("'", '"', "`"):
            in_string = chars[i]
            cleaned.append(chars[i])
            mapping.append(i)
            i += 1
            continue
            
        if i + 1 < n and chars[i] == '-' and chars[i+1] == '-':
            i += 2
            while i < n and chars[i] != '\n':
                i += 1
            if i < n and chars[i] == '\n':
                cleaned.append('\n')
                mapping.append(i)
                i += 1
            continue
            
        if i + 1 < n and chars[i] == '/' and chars[i+1] == '*':
            i += 2
            while i + 1 < n and not (chars[i] == '*' and chars[i+1] == '/'):
                i += 1
            i += 2
            continue
            
        cleaned.append(chars[i])
        mapping.append(i)
        i += 1
        
    return "".join(cleaned), mapping

def normalize_with_map(s: str) -> tuple[str, list[int]]:
    normalized = []
    mapping = []
    for idx, c in enumerate(s):
        if not c.isspace() and c != ';':
            normalized.append(c.lower())
            mapping.append(idx)
    return "".join(normalized), mapping

def get_preceding_comments(sql: str, start_pos: int) -> list[str]:
    lines_before = sql[:start_pos].splitlines()
    comments = []
    found_comment = False
    for line in reversed(lines_before[-6:]):
        stripped = line.strip()
        is_comment = stripped.startswith('--') or (stripped.startswith('/*') and stripped.endswith('*/'))
        if is_comment:
            comments.append(line)
            found_comment = True
        elif found_comment:
            break
    return list(reversed(comments))

def rewrite_query(sql_text: str) -> str:
    cleaned_sql, clean_to_orig_map = strip_comments_preserving_positions(sql_text)
    parsed = sqlvalidator.parse(cleaned_sql)
    if not parsed.is_valid():
        print("Warning: Input SQL query is invalid:")
        for err in parsed.errors:
            print(f"  - {err}")
            
    root = parsed.sql_query
    
    norm_cleaned, norm_to_clean_map = normalize_with_map(cleaned_sql)
    
    rewriter = CTERewriter()
    new_root = rewriter.rewrite(root)

    matched_positions = set()
    with_queries = []
    for name, stmt in rewriter.ctes:
        stmt_sql = getattr(stmt, "_original_sql", None) or transform(stmt)
        norm_cte, _ = normalize_with_map(stmt_sql)
        
        pos = norm_cleaned.find(norm_cte)
        while pos != -1 and pos in matched_positions:
            pos = norm_cleaned.find(norm_cte, pos + 1)
            
        comments = []
        if pos != -1:
            matched_positions.add(pos)
            clean_idx = norm_to_clean_map[pos]
            orig_idx = clean_to_orig_map[clean_idx]
            comments = get_preceding_comments(sql_text, orig_idx)
            
        with_queries.append(CommentedWithQuery(name, stmt, comments))

    if with_queries:
        if isinstance(new_root, WithStatement):
            new_root.with_queries = with_queries + new_root.with_queries
            final_query = new_root
        else:
            final_query = WithStatement(with_queries, new_root)
    else:
        final_query = new_root

    return transform(final_query)

if __name__ == "__main__":
    query = """
    SELECT hello FROM world
    """
    print(rewrite_query(query))