from py2neo import Graph, Node, Relationship, Subgraph
import pymysql, neo

graph = Graph(
    "http://39.100.224.138:17474//",
    username="neo4j",
    password="icdathings"
)
mysql_config = {'host': "39.100.224.138",
                'user': "root",
                'password': "slx.seu@2020",
                'db': 'jiang_db',
                'port': 3306,
                'charset': 'utf8mb4'}
conn = pymysql.connect(**mysql_config)
cursor = conn.cursor()

sql1 = """SELECT ep_product.ep_id,ep_product.product_code FROM ep_product """
cursor.execute(sql1)
result = cursor.fetchall()
count = len(result)
ep_id_ex = []
product_id_ex = []

for i in range(count):
    ep_id = result[i][0]
    product_id = result[i][1]
    a = graph.nodes.match("enterprise", enterprise_id='ep' + str(ep_id)).first()
    b = graph.nodes.match("product", product_id='p' + product_id).first()
    if a == None:
        if (ep_id in ep_id_ex):
            pass
        else:
            ep_id_ex.append(ep_id)
            print(ep_id)
    if b == None:
        if (product_id in product_id_ex):
            pass
        else:
            product_id_ex.append(product_id)
            print(product_id)

