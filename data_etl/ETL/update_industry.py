from py2neo import Graph, Node, Relationship, NodeMatcher
import pymysql

graph = Graph("http://101.201.125.180:7474/", username="neo4j", password="icdathings")

conn = pymysql.connect(host="101.201.125.180", user="root", password="shuinori", db='slx_db', port=3306,
                       charset='utf8mb4')
cursor = conn.cursor()
sql = """SELECT * FROM industry WHERE depth = 2"""
cursor.execute(sql)
result = cursor.fetchall()


def findNode(name, graph):
    matcher = NodeMatcher(graph)
    m = matcher.match(industry_name=name).first()  # 使用nodematcher找到节点
    return m


t = 1

for i in result:
    industry = findNode(i[1], graph)
    industry.update({'level': i[3]})
    graph.push(industry)
    print('更新成功' + str(t))
    t = t + 1
