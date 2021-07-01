from clickhouse_driver import Client

clickhouse_config = {'host': "39.100.224.138",
                     'port': '9090',
                     'database': 'JIANG',
                     'user': "default",
                     'password': "slx2021"
                     }
client = Client(**clickhouse_config)


def query_ep(level):
    query_sql = """SELECT * FROM product WHERE `level` ={level}""".format(level=level)
    print(query_sql)
    try:
        product_list = client.execute(query_sql)
    except Exception as e:
        print(e)
    return product_list


def query_product_name_by_code(code):
    query_sql = """SELECT title,parent_code FROM product WHERE `code` = '{code}' """.format(code=code)
    try:
        product_info = client.execute(query_sql)
    except Exception as e:
        print(e)
    return product_info[0]


def insert_data(product_weidu):
    insert_sql = "INSERT INTO product_weidu VALUES ('{}','{}','{}','{}','{}','{}');".format(
        product_weidu['product_code'],
        product_weidu['product_level1'],
        product_weidu["product_level2"],
        product_weidu['product_level3'],
        product_weidu['product_level4'],
        product_weidu['product_level5'],
    )
    print(insert_sql)
    try:
        client.execute(insert_sql)
    except Exception as e:
        print('插入失败')
        print(e)


if __name__ == "__main__":
    product5list = query_ep(level=5)
    product_list = []
    for i in range(len(product5list)):
        product_weidu = {"product_code": '',
                         "product_level1": '',
                         "product_level2": '',
                         "product_level3": '',
                         "product_level4": '',
                         "product_level5": '',
                         }
        product_weidu['product_code'] = product5list[i][0]
        product_weidu['product_level5'] = product5list[i][1]
        parent_product4 = query_product_name_by_code(product5list[i][3])
        product_weidu['product_level4'] = parent_product4[0]
        parent_product3 = query_product_name_by_code(parent_product4[1])
        product_weidu['product_level3'] = parent_product3[0]
        parent_product2 = query_product_name_by_code(parent_product3[1])
        product_weidu['product_level2'] = parent_product2[0]
        product_weidu['product_level1'] = query_product_name_by_code(parent_product2[1])[0]
        insert_data(product_weidu)
