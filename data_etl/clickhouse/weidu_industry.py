from clickhouse_driver import Client

clickhouse_config = {'host': "39.100.224.138",
                     'port': '9090',
                     'database': 'JIANG',
                     'user': "default",
                     'password': "slx2021"
                     }
client = Client(**clickhouse_config)


def query_ep(depth):
    query_sql = """SELECT * FROM industry WHERE `depth` ={depth}""".format(depth=depth)
    print(query_sql)
    try:
        industry_list = client.execute(query_sql)
    except Exception as e:
        print(e)
    return industry_list


def query_ep_by_code(code):
    query_sql = """SELECT * FROM industry WHERE `code` = '{code}' """.format(code=code)
    try:
        industry_info = client.execute(query_sql)
    except Exception as e:
        print(e)
    return industry_info


def insert_data(industry_weidu):
    insert_sql = "INSERT INTO industry_weidu VALUES ('{}','{}','{}','{}');".format(industry_weidu['industry_code'],
                                                                                   industry_weidu["industry_level1"],
                                                                                   industry_weidu["industry_level2"],
                                                                                   industry_weidu['industry_level3'])
    print(insert_sql)
    try:
        client.execute(insert_sql)
    except Exception as e:
        print('插入失败')
        print(e)


if __name__ == "__main__":
    industry3list = query_ep(depth=1)
    for i in range(len(industry3list)):
        industry_weidu = {"industry_code": "",
                          "industry_level1": "",
                          "industry_level2": None,
                          "industry_level3": None
                          }
        industry2_info = query_ep_by_code(str(industry3list[i][4]))[0]
        industry_weidu['industry_code'] = industry3list[i][0]
        industry_weidu["industry_level1"] = industry3list[i][1]
        insert_data(industry_weidu)
