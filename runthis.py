import sqlvalidator
from sqlvalidator.grammar.sql import Parenthesis, SelectStatement, Table

sql_query = sqlvalidator.parse("""

select *, row_number() over(partition by a order by b desc) row_num from (select a,b,c from  (select * from table_x where family = 'Smiths')  ) 

""")

def graph_it(sql_query):
    nodes = {}
    edges = {}
    def parse_tree(sql_query, counter=0):
        nodes[counter] = sql_query
        if isinstance(sql_query.from_statement, Parenthesis) and isinstance(sql_query.from_statement.args[0], SelectStatement):
            edges[counter] = counter + 1
            counter += 1
            return parse_tree(sql_query.from_statement.args[0], counter)
        return (nodes, edges)
    return parse_tree(sql_query)

nodes, edges = graph_it(sql_query.sql_query)

for index, node in enumerate(reversed(nodes), start=1):
    select_statement: SelectStatement = nodes[node]
    innermost_select = node not in edges
    outermost_select = node == list(nodes.keys())[0]
    if not innermost_select:
        select_statement.from_statement = Table(f"cte_{index-1}")
    print(f"WITH cte_{index} AS (")
    print(select_statement.transform())
    
    print(")", end='')
    if not outermost_select:
        print(",")
print()
# print(sql_query.sql_query)