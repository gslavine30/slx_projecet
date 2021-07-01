from xml.dom.minidom import parse
import xml.dom.minidom
import pymysql

# 使用minidom解析器打开 XML 文档
DOMTree = xml.dom.minidom.parse("product.xml")
collection = DOMTree.documentElement
nodes = collection.getElementsByTagName("node")

products = []

for node in nodes:
    code = node.getAttribute('id')
    name = node.getAttribute('name')
    level = node.getAttribute('level')
    parentcode = code
    if level == "5":
        parentcode = parentcode[:-2]
    elif level == "4":
        parentcode = parentcode[:-2]
    elif level == "3":
        parentcode = parentcode[:-2]
    elif level == "2":
        parentcode = parentcode[:-2]
    elif level == "1":
        parentcode = None
    product = {'id': code, 'name': name, 'level': int(level), 'parentcode': parentcode}
    products.append(product)

print(len(products))

# conn = pymysql.connect(host="101.201.125.180", user="root", password="shuinori", db='slx_db', port=3306,
#                        charset='utf8mb4')
# cursor = conn.cursor()
# for num in range(30209, len(products)):
#     productsql = products[num]
#     try:
#         sql = """INSERT INTO product VALUES (%s, %s,%s,%s)"""
#         cursor.execute(sql, (productsql['id'], productsql['name'], productsql['level'], productsql['parentcode']))
#         conn.commit()
#         print('插入成功')
#     except Exception as e:
#         print(e)
#         conn.rollback()
#         print('插入失败')
# conn.close()
