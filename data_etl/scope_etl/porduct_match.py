import pymysql

conn = pymysql.connect(host="101.201.125.180", user="root", password="shuinori", db='slx_db', port=3306,
                       charset='utf8')
cursor = conn.cursor()


def get_product_list():
    """
    获取到产品列表
    :return:
    """
    sql = """
        SELECT code,title FROM `product`
    """
    cursor.execute(sql)
    result = cursor.fetchall()
    products = []
    for i in result:
        product = {'code': i[0],
                   'title': i[1]}
        products.append(product)
    return products


def get_scope(nums=10):
    """
    获取到企业经营范围
    :return:
    """
    sql = """
        select id,scope from ep_base_info where limit {}
    """.format(nums)
    cursor.execute(sql)
    result = cursor.fetchall()
    ep_scopes = []
    for i in result:
        ep_scope = {'id': i[0],
                    'scope': i[1]}
        ep_scopes.append(ep_scope)
    return ep_scopes


def insert_ep_product_rel():
    """
    将产品和经营范围的关系插入到数据库中
    :return:
    """
    products = get_product_list()
    scopes = get_scope(45000)
    print("读取完毕")
    count = len(scopes)
    for i in range(count):

        product_list = []
        for p in products:
            if scopes[i]['scope'].__contains__(p['title']):
                product_list.append((scopes[i]['id'], p['code']))
        if product_list != []:
            sql = """INSERT INTO ep_product VALUES (%s,%s)"""
            try:
                cursor.executemany(sql, product_list)
                conn.commit()
                print('{}/{}:插入成功'.format(count, i + 1))
            except Exception as e:
                print(e)
                conn.rollback()
                print('插入失败')
        else:
            pass
    return "插入完毕"


if __name__ == "__main__":
    pass
