from py2neo import Graph, Node, Relationship
import pymysql

graph = Graph(
    "http://39.100.224.138:17474//",
    username="neo4j",
    password="icdathings"
)
conn = pymysql.connect(host="101.201.125.180", user="root", password="shuinori", db='slx_db', port=3306,
                       charset='utf8mb4')
cursor = conn.cursor()


def insert_node(level):
    sql1 = """SELECT * FROM product WHERE LEVEL = %s"""
    sql2 = """SELECT count(*) FROM product WHERE LEVEL = %s"""
    cursor.execute(sql1, level)
    result = cursor.fetchall()
    cursor.execute(sql2, level)
    count = cursor.fetchone()
    count = count[0]
    t = 1

    for i in result:
        '''图数据库上创建产品节点'''
        product_name = i[1]
        product_id = 'p' + i[0]
        product_node = Node("product", product_name=product_name, product_id=product_id, product_level=i[2])
        graph.create(product_node)
        t += 1
        print(str(t) + '/' + str(count) + ':产品：' + i[1] + '插入成功')


def insert_rel(level, start=0):
    '''
    图数据库上创建关系



    '''
    sql1 = """SELECT * FROM product WHERE LEVEL = %s"""
    sql2 = """SELECT count(*) FROM product WHERE LEVEL = %s"""
    cursor.execute(sql1, level)
    result = cursor.fetchall()
    cursor.execute(sql2, level)
    count = cursor.fetchone()
    count = count[0]
    t = start + 1
    for j in range(start, len(result)):
        i = result[j]
        product_id_a = 'p' + i[0]
        product_id_b = 'p' + i[3]
        a = graph.nodes.match("product", product_id=product_id_a).first()
        b = graph.nodes.match("product", product_id=product_id_b).first()
        rel = Relationship(a, "属于（产品）", b)
        graph.create(rel)
        t += 1
        print(str(t) + '/' + str(count) + ':产品：' + i[1] + '关系插入成功')


insert_rel(level=4, start=6731)
