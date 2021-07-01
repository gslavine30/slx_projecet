import pymysql

conn = pymysql.connect(host="101.201.125.180", user="root", password="shuinori", db='slx_db', port=3306,
                       charset='utf8')
cursor = conn.cursor()


def creat_jieba_dict():
    """
    创建自定义的jieba分词表
    :return:
    """
    sql = """
        SELECT title FROM `product`
    """
    cursor.execute(sql)
    result = cursor.fetchall()
    count = len(result)

    with open("dict.txt", "w", encoding='utf-8') as f:
        for i in range(count):
            print('插入第{}/{}个成功'.format(i + 1, count))
            product_name = result[i][0]
            f.write(product_name + '\n')

