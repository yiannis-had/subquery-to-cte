import sqlvalidator
from sqlvalidator.grammar.sql import Parenthesis, SelectStatement, WithStatement, Table, Alias
from collections import deque

sql_query = sqlvalidator.parse("""
select *, row_number() over(partition by a order by b desc) row_num from (select a,b,c from  (select * from table_x where family = 'Smiths';)a )b
""")

def graph_it(sql_query: SelectStatement | WithStatement):
    nodes = {}
    edges = {}
    edges_stack = []
    def parse_tree(sql_query, counter=0):
        print(sql_query.__repr__())
        print('-------------------')
        if isinstance(sql_query, Alias) and isinstance(sql_query.expression, Parenthesis):
            alias = sql_query.alias
            sql_query = sql_query.expression
        else:
            alias = None
        if isinstance(sql_query, Parenthesis) and isinstance(sql_query.args[0], SelectStatement):
            sql_query = sql_query.args[0]
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

for index, node in enumerate(reversed(nodes_keys)):
    node_index = None if len(nodes_keys) - index else None
    select_statement: SelectStatement = nodes[node]
    innermost_select = node not in edges
    outermost_select = node == nodes_keys[0]
    before_outermost_select = node == nodes_keys[1]
    if not innermost_select:
        select_statement.from_statement = Table(f"cte_{index-1}")


    if innermost_select:
        print(f"WITH cte_{index} AS (")
        print(select_statement.transform())
        print("),")
    if not innermost_select and not outermost_select:
        print(f"cte_{index} AS (")
        print(select_statement.transform())
        if not before_outermost_select:
            print("),")
        else:
            print(")")
    elif outermost_select:
        print(select_statement.transform())

