import sqlvalidator
from sqlvalidator.grammar.sql import SelectStatement, WithStatement, Table, Alias, Parenthesis, transform

sql_query = sqlvalidator.parse("""
select *, row_number() over(partition by a order by b desc) row_num from (select a,b,c from  (select * from table_x where family = 'Smiths';)a )b
""")

def graph_it(sql_query: SelectStatement | WithStatement):
    nodes = {}
    def parse_tree(sql_query, counter=0):
        if isinstance(sql_query, Alias) and isinstance(sql_query.expression, Parenthesis):
            alias = sql_query.alias
            sql_query = sql_query.expression
        else:
            alias = None
        if isinstance(sql_query, Parenthesis) and isinstance(sql_query.args[0], SelectStatement):
            sql_query = sql_query.args[0]
        key = alias if alias else counter
        nodes[key] = sql_query
        if alias is None:
            counter += 1
        if hasattr(sql_query, 'from_statement') and not isinstance(sql_query.from_statement, Table):
            return parse_tree(sql_query.from_statement, counter)
        return nodes
    return parse_tree(sql_query)

nodes = graph_it(sql_query.sql_query)
nodes_keys = list(nodes.keys())
reversed_nodes_keys = list(reversed(nodes_keys))
print('WITH ', end='')
for index, node in enumerate(reversed_nodes_keys):
    prev_node = None if index == 0 else reversed_nodes_keys[index-1]
    select_statement: SelectStatement = nodes[node]
    innermost_select = node == reversed_nodes_keys[0]
    outermost_select = node == nodes_keys[0]
    before_outermost_select = node == nodes_keys[1]
    aliased_select = isinstance(node, str)
    prev_node_aliased_select = isinstance(prev_node, str)
    if not innermost_select:
        select_statement.from_statement = Table(f"{prev_node}") if prev_node_aliased_select else Table(f"cte_{prev_node}") 

    if not outermost_select:
        print(f"{node} AS (") if aliased_select else print(f"cte_{node} AS (")
        print(transform(select_statement))
        if not before_outermost_select:
            print("),")
        else:
            print(")")
    else:
        print(transform(select_statement))

