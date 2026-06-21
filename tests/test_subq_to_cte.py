import warnings

from sqlvalidator import parse
from sqlvalidator.grammar.sql import SelectStatement, Table
from subq_to_cte import (
    CommentedWithQuery,
    CTERewriter,
    TableCollector,
    collect_tables,
    get_preceding_comments,
    normalize_with_map,
    rewrite_query,
    strip_comments_preserving_positions,
)


def _extract_cte_names(result: str) -> list[str]:
    """Return the CTE names declared in a WITH clause, in order."""
    names: list[str] = []
    for line in result.splitlines():
        stripped = line.strip()
        if " AS (" in stripped.upper() or " AS\n(" in stripped.upper():
            name = stripped.split(" AS", 1)[0].strip()
            name = name.removeprefix("WITH ").strip()
            if name:
                names.append(name)
    return names


class TestRewriteQuery:
    """End-to-end tests for rewrite_query."""

    def test_simple_query_no_subqueries(self):
        """A plain SELECT with no subqueries should pass through unchanged."""
        sql = "SELECT id, name FROM users WHERE active = true;"
        result = rewrite_query(sql)
        assert "users" in result.lower()
        assert "WITH" not in result
        assert _extract_cte_names(result) == []

    def test_from_subquery_extracted(self):
        """Subquery in FROM clause becomes a CTE."""
        sql = "SELECT * FROM (SELECT id FROM users) u;"
        result = rewrite_query(sql)
        names = _extract_cte_names(result)
        assert len(names) == 1
        assert names[0] == "u"
        assert "SELECT id" in result
        assert "FROM users" in result

    def test_nested_subqueries(self):
        """Subquery within a subquery: both should be extracted."""
        sql = (
            "SELECT * FROM ("
            "    SELECT customer_id FROM orders "
            "    WHERE order_id IN (SELECT order_id FROM order_items WHERE quantity > 1)"
            ") sub;"
        )
        result = rewrite_query(sql)
        names = _extract_cte_names(result)
        assert len(names) == 2
        assert "order_items" in result

    def test_where_in_subquery(self):
        """WHERE ... IN (subquery) should extract the subquery."""
        sql = (
            "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM vip_list);"
        )
        result = rewrite_query(sql)
        names = _extract_cte_names(result)
        assert len(names) == 1
        assert "vip_list" in result

    def test_join_subquery(self):
        """JOIN (subquery) should extract the subquery."""
        sql = (
            "SELECT c.name, o.total "
            "FROM customers c "
            "JOIN (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id) o "
            "ON o.customer_id = c.id;"
        )
        result = rewrite_query(sql)
        names = _extract_cte_names(result)
        assert len(names) == 1
        assert names[0] == "o"
        assert "orders" in result

    def test_alias_based_cte_naming(self):
        """CTE name should derive from the subquery alias when available."""
        sql = "SELECT * FROM (    SELECT id FROM some_table) recent_orders;"
        result = rewrite_query(sql)
        names = _extract_cte_names(result)
        assert len(names) == 1
        assert names[0] == "recent_orders"

    def test_alias_collides_with_table_name(self):
        """When alias matches a table name, a numbered suffix is added."""
        sql = "SELECT * FROM (    SELECT id FROM recent_orders) recent_orders;"
        result = rewrite_query(sql)
        names = _extract_cte_names(result)
        assert len(names) == 1
        assert names[0] == "recent_orders_1"

    def test_name_collision_avoidance(self):
        """CTE names should not shadow existing table names."""
        sql = "SELECT * FROM (    SELECT id FROM orders) orders;"
        result = rewrite_query(sql)
        names = _extract_cte_names(result)
        assert len(names) == 1
        assert names[0] != "orders", (
            "CTE should not be named 'orders' — that's an existing table"
        )
        assert names[0].startswith("orders_")

    def test_query_with_existing_with_clause(self):
        """Queries that already have WITH clauses should prepend new CTEs."""
        sql = (
            "WITH existing_cte AS (SELECT 1 AS n) "
            "SELECT * FROM existing_cte "
            "JOIN (SELECT id FROM users) u ON u.id = existing_cte.n;"
        )
        result = rewrite_query(sql)
        assert "WITH" in result
        names = _extract_cte_names(result)
        assert "existing_cte" in names
        assert "u" in names

    def test_comment_propagation(self):
        """Comments preceding a subquery should appear above the CTE."""
        sql = """
SELECT * FROM (
    -- this is the inner query
    SELECT id FROM users
) u;
"""
        result = rewrite_query(sql)
        assert "-- this is the inner query" in result

    def test_multiple_subqueries_produce_multiple_ctes(self):
        """Multiple subqueries should each get their own CTE."""
        sql = (
            "SELECT a.id, b.name "
            "FROM (SELECT id FROM t1) a "
            "JOIN (SELECT name FROM t2) b ON a.id = b.id;"
        )
        result = rewrite_query(sql)
        names = _extract_cte_names(result)
        assert len(names) == 2
        assert "a" in names
        assert "b" in names

    def test_no_alias_fallback_cte_numbering(self):
        """Subqueries without aliases should get cte_1, cte_2, etc."""
        sql = (
            "SELECT name FROM customers "
            "WHERE id IN (SELECT customer_id FROM vip_list) "
            "AND status IN (SELECT status FROM statuses);"
        )
        result = rewrite_query(sql)
        names = _extract_cte_names(result)
        assert len(names) == 2
        assert "cte_1" in names
        assert "cte_2" in names


class TestCollectTables:
    """Tests for collect_tables function."""

    def test_collects_table_names(self):
        sql = "SELECT * FROM orders JOIN customers ON orders.cid = customers.id;"
        parsed = parse(sql)
        tables = collect_tables(parsed.sql_query)
        assert "orders" in tables
        assert "customers" in tables

    def test_handles_none(self):
        tables = collect_tables(None)
        assert tables == set()


class TestTableCollector:
    """Tests for the backward-compatible TableCollector wrapper."""

    def test_collects_table_names(self):
        sql = "SELECT * FROM orders JOIN customers ON orders.cid = customers.id;"
        parsed = parse(sql)
        collector = TableCollector()
        collector.collect(parsed.sql_query)
        assert "orders" in collector.tables
        assert "customers" in collector.tables

    def test_warns_on_unknown_node_type(self):
        collector = TableCollector()

        class WeirdNode:
            __slots__ = ()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            collector.collect(WeirdNode())
            assert len(w) == 1
            assert "WeirdNode" in str(w[0].message)


class TestCTERewriter:
    """Tests for CTERewriter."""

    def test_get_cte_name_no_alias(self):
        rewriter = CTERewriter()
        name = rewriter.get_cte_name()
        assert name == "cte_1"
        name = rewriter.get_cte_name()
        assert name == "cte_2"

    def test_get_cte_name_with_alias(self):
        rewriter = CTERewriter()
        name = rewriter.get_cte_name(alias="recent_orders")
        assert name == "recent_orders"

    def test_get_cte_name_collision(self):
        rewriter = CTERewriter()
        rewriter.used_names.add("orders")
        name = rewriter.get_cte_name(alias="orders")
        assert name == "orders_1"

    def test_get_cte_name_collision_numbering(self):
        rewriter = CTERewriter()
        rewriter.used_names.update(["orders", "orders_1", "orders_2"])
        name = rewriter.get_cte_name(alias="orders")
        assert name == "orders_3"

    def test_get_cte_name_cleans_special_chars(self):
        rewriter = CTERewriter()
        name = rewriter.get_cte_name(alias="my-table!name")
        assert name == "my_table_name"

    def test_used_names_populated_from_tables(self):
        """After rewrite, table names should be reserved in used_names."""
        sql = "SELECT * FROM (SELECT id FROM users) u;"
        parsed = parse(sql)
        rewriter = CTERewriter()
        rewriter.rewrite(parsed.sql_query)
        assert "users" in rewriter.used_names

    def test_original_sql_stored_in_cte_tuple(self):
        """CTERewriter stores the original (pre-rewrite) SQL as a plain str in each CTE tuple."""
        sql = "SELECT * FROM (SELECT id FROM users) u;"
        parsed = parse(sql)
        rewriter = CTERewriter()
        rewriter.rewrite(parsed.sql_query)
        assert len(rewriter.ctes) == 1
        _name, orig_sql, _stmt = rewriter.ctes[0]
        assert isinstance(orig_sql, str)
        assert "id" in orig_sql
        assert "users" in orig_sql


class TestCommentedWithQuery:
    """Tests for CommentedWithQuery."""

    def test_transform_without_comments(self):
        stmt = SelectStatement(expressions=[], from_statement=Table("t"))
        cq = CommentedWithQuery("my_cte", stmt, [])
        result = cq.transform()
        assert "my_cte AS (" in result

    def test_transform_with_comments(self):
        stmt = SelectStatement(expressions=[], from_statement=Table("t"))
        cq = CommentedWithQuery("my_cte", stmt, ["-- important comment"])
        result = cq.transform()
        assert "-- important comment" in result
        assert "my_cte AS (" in result

    def test_str_delegates_to_transform(self):
        stmt = SelectStatement(expressions=[], from_statement=Table("t"))
        cq = CommentedWithQuery("my_cte", stmt, ["-- note"])
        assert str(cq) == cq.transform()


class TestHelpers:
    """Tests for helper functions."""

    def test_strip_comments_preserves_strings(self):
        sql = "SELECT '-- not a comment' FROM t;"
        cleaned, mapping = strip_comments_preserving_positions(sql)
        assert "'-- not a comment'" in cleaned

    def test_strip_comments_removes_line_comment(self):
        sql = "SELECT id -- comment\nFROM t;"
        cleaned, _ = strip_comments_preserving_positions(sql)
        assert "-- comment" not in cleaned
        assert "SELECT id" in cleaned
        assert "FROM t" in cleaned

    def test_strip_comments_removes_block_comment(self):
        sql = "SELECT id /* block */ FROM t;"
        cleaned, _ = strip_comments_preserving_positions(sql)
        assert "/* block */" not in cleaned

    def test_normalize_with_map(self):
        sql = "SELECT id FROM t;"
        normalized, mapping = normalize_with_map(sql)
        assert normalized == "selectidfromt"
        assert len(mapping) == len(normalized)

    def test_get_preceding_comments(self):
        sql = """-- comment line 1
-- comment line 2
SELECT * FROM t;"""
        pos = sql.index("SELECT")
        comments = get_preceding_comments(sql, pos)
        assert "-- comment line 1" in comments
        assert "-- comment line 2" in comments


class TestRewriteWarnings:
    """Test that invalid SQL produces warnings."""

    def test_invalid_sql_warns(self):
        """Unparseable SQL should emit a warning and return input unchanged."""
        bad_sql = "SELECTT FROM t;"
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = rewrite_query(bad_sql)
            syntax_warnings = [x for x in w if issubclass(x.category, SyntaxWarning)]
            assert len(syntax_warnings) >= 1
            assert result == bad_sql

    def test_valid_sql_no_warning(self):
        sql = "SELECT id FROM users;"
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            rewrite_query(sql)
            syntax_warnings = [x for x in w if issubclass(x.category, SyntaxWarning)]
            assert len(syntax_warnings) == 0


class TestReadmeExample:
    """Regression test matching the README example."""

    def test_readme_example(self):
        sql = """\
SELECT c.name, spending.total
FROM customers c
JOIN (
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    WHERE order_id IN (
        SELECT order_id FROM order_items WHERE quantity > 1
    )
    GROUP BY customer_id
) spending ON spending.customer_id = c.id
WHERE c.id IN (
    SELECT customer_id FROM vip_list
)"""
        result = rewrite_query(sql)
        assert result.startswith("WITH ")
        names = _extract_cte_names(result)
        assert len(names) == 3
        assert "spending" in names
        assert "order_items" in result
        assert "vip_list" in result
        # The CTE that wraps the inner-most subquery should be referenced
        # inside the spending CTE body.
        spending_idx = result.index("spending AS (")
        inner_cte_name = names[0]
        inner_ref = result.find(inner_cte_name, spending_idx)
        assert inner_ref != -1
