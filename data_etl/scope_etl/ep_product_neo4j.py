from py2neo import Graph, Node, Relationship, Subgraph
import pymysql

graph = Graph(
    "http://39.100.224.138:17474//",
    auth=("neo4j", "icdathings"),
)

mysql_config = {'host': "39.100.224.138",
                'user': "root",
                'password': "slx.seu@2020",
                'db': 'jiang_db',
                'port': 3306,
                'charset': 'utf8mb4'}
conn = pymysql.connect(**mysql_config)
cursor = conn.cursor()


def insert_ep_node():
    sql1 = """SELECT ep_base_info.id, ep_base_info.`name` FROM ep_base_info WHERE id IN ( SELECT DISTINCT ep_id FROM ep_product ) """
    cursor.execute(sql1)
    result = cursor.fetchall()
    count = len(result)
    graph_cursor = graph.begin()
    for i in range(count):
        ep = result[i]
        '''图数据库上创建产品节点'''
        ep_node = Node("enterprise", enterprise_name=ep[1], enterprise_id='ep' + str(ep[0]))
        graph_cursor.create(ep_node)
        print('{}/{}:创建成功'.format(count, i + 1))
    graph_cursor.commit()


def insert_ep_rel(start = 0):
    sql1 = """SELECT ep_product.ep_id,ep_product.product_code FROM ep_product """
    cursor.execute(sql1)
    result = cursor.fetchall()
    count = len(result)
    pro_list = []
    for i in range(start,count):
        ep_id = result[i][0]
        product_id = result[i][1]
        a = graph.nodes.match("enterprise", enterprise_id='ep' + str(ep_id)).first()
        b = graph.nodes.match("product", product_id='p' + product_id).first()
        if a != None and b != None:
            rel = Relationship(a, "经营", b)
            graph.create(rel)
            print('{}/{}:创建成功'.format(count, i + 1))
        # if b == None:
        #     if (product_id in pro_list):
        #         pass
        #     else:
        #         pro_list.append(product_id)
        #         sql2 = """INSERT INTO product_test VALUES (%s)"""
        #         cursor.execute(sql2, product_id)
        #         conn.commit()
        #     print('{}/{}:创建失败，插入成功'.format(count, i + 1))


if __name__ == "__main__":
    insert_ep_rel(start=120530)
