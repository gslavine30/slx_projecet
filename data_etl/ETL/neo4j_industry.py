from py2neo import Graph, Node, Relationship
import pymysql

graph = Graph(
    "http://39.100.224.138:17474//",
    username="neo4j",
    password="icdathings"
)

conn = pymysql.connect(host="101.201.125.180", user="root", password="shuinori", db='slx_db', port=3306,
                       charset='utf8mb4')

t = 0


def insert_node(level):
    cursor = conn.cursor()
    sql1 = """SELECT * FROM industry WHERE depth = %s"""
    sql2 = """SELECT count(*) FROM industry WHERE depth = %s"""
    cursor.execute(sql1, level)
    result = cursor.fetchall()
    cursor.execute(sql2, level)
    count = cursor.fetchone()
    count = count[0]
    t = 1
    for i in result:
        industry_name = i[1]
        industry_id = 'i' + i[0]
        industry_node = Node("industry", industry_name=industry_name, industry_id=industry_id, industry_level=i[3],
                             industry_discription=i[2])
        graph.create(industry_node)
        t = t + 1
        print(str(t) + '/' + str(count) + ':行业：' + i[1] + '插入成功')


def insert_rel(level):
    cursor = conn.cursor()
    sql1 = """SELECT * FROM industry WHERE depth = %s"""
    sql2 = """SELECT count(*) FROM industry WHERE depth = %s"""
    cursor.execute(sql1, level)
    result = cursor.fetchall()
    cursor.execute(sql2, level)
    count = cursor.fetchone()
    count = count[0]
    t = 1
    for i in result:
        '''图数据库上创建节点和关系'''
        industry_a = 'i' + i[0]
        industry_b = 'i' + i[4]
        a = graph.nodes.match("industry", industry_id=industry_a).first()
        b = graph.nodes.match("industry", industry_id=industry_b).first()
        rel = Relationship(a, "属于（行业）", b)
        graph.create(rel)
        print(str(t) + '/' + str(count) + ':行业：' + i[1] + '关系插入成功')
        t = t + 1
