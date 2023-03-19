import sqlvalidator
from sqlvalidator.grammar.sql import Parenthesis, SelectStatement, WithStatement, Table, Alias

sql_query = sqlvalidator.parse("""
select *, row_number() over(partition by a order by b desc) row_num from (select a,b,c from  (select * from table_x where family = 'Smiths';)a )b
""")

def graph_it(sql_query: SelectStatement | WithStatement):
    nodes = {}
    edges = {}
    edges_stack = []
    def parse_tree(sql_query, counter=0):
        if isinstance(sql_query, Alias) and isinstance(sql_query.expression, Parenthesis):
            alias = sql_query.alias
            sql_query = sql_query.expression
            if isinstance(sql_query, Parenthesis) and isinstance(sql_query.args[0], SelectStatement):
                sql_query = sql_query.args[0]
        else:
            alias = None
        key = alias if alias else counter
        nodes[key] = sql_query
        if edges_stack:
            prev_key = edges_stack.pop()
            edges[prev_key] = key
        edges_stack.append(key)
        if alias is None:
            counter += 1
        if hasattr(sql_query, 'from_statement') and not isinstance(sql_query.from_statement, Table):
            return parse_tree(sql_query.from_statement, counter)
        return (nodes, edges)
    return parse_tree(sql_query)

nodes, edges = graph_it(sql_query.sql_query)
nodes_keys = list(nodes.keys())
reversed_nodes_keys = list(reversed(nodes_keys))
for index, node in enumerate(reversed_nodes_keys):
    prev_node = None if index == 0 else reversed_nodes_keys[index-1]
    select_statement: SelectStatement = nodes[node]
    innermost_select = node not in edges
    outermost_select = node == nodes_keys[0]
    before_outermost_select = node == nodes_keys[1]
    aliased_select = isinstance(node, str)
    prev_node_aliased_select = isinstance(prev_node, str)
    if not innermost_select:
        select_statement.from_statement = Table(f"{prev_node}") if prev_node_aliased_select else Table(f"cte_{prev_node}") 

    if innermost_select:
        print(f"WITH {node} AS (") if aliased_select else print(f"WITH cte_{node} AS (")
        print(select_statement.transform())
        print("),")
    if not innermost_select and not outermost_select:
        print(f"{node} AS (") if aliased_select else print(f"cte_{node} AS (")
        print(select_statement.transform())
        if not before_outermost_select:
            print("),")
        else:
            print(")")
    elif outermost_select:
        print(select_statement.transform())

