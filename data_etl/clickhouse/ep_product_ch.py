from clickhouse_driver import Client

clickhouse_config = {'host': "39.100.224.138",
                     'port': '9090',
                     'database': 'JIANG',
                     'user': "default",
                     'password': "slx2021"
                     }
client = Client(**clickhouse_config)


def query_ep():
    # sql1 = """select epi.id,epi.`scope` from ep_base_info epi WHERE `scope` LIKE '%机动车制动系统%' OR `scope` LIKE '%机动车零部件%制造' OR `scope` LIKE '%机动车零部件及配件%制造' OR `scope` LIKE '%机动车配件%制造'"""
    sql1 = """select epi.id,epi.`scope`,epi.name from ep_base_info epi WHERE `scope` LIKE '%汽车%底盘%'"""
    try:
        list = client.execute(sql1)
    except Exception as e:
        print(e)
    return list


def insert_rel(code,rel_list):
    for ep in rel_list:
        insert_sql = "INSERT INTO ep_product VALUES ({} ,{})".format(ep[0], code)
        try:
            client.execute(insert_sql)
        except Exception as e:
            print('插入失败')
            print(e)

    return ''


list = query_ep()
print(len(list))
for ep in list:
    print(ep[2])
    print(ep[1])

insert_rel(code='370505',rel_list=list)
