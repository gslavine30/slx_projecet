import pymysql

conn = pymysql.connect(host="101.201.125.180", user="root", password="shuinori", db='slx_db', port=3306,
                       charset='utf8mb4')
cursor = conn.cursor()

sql = """SELECT * FROM product WHERE LEVEL = 5"""
cursor.execute(sql)
result = cursor.fetchall()


for i in result:
    i = list(i)
    if len(i[3]) != 8:
        # i[3] = i[3][0:6]
        print(i)
    #     try:
    #         sql = """UPDATE product SET parent_code = %s WHERE code = %s"""
    #         cursor.execute(sql, (i[3], i[0]))
    #         conn.commit()
    #         print('修改成功')
    #     except Exception as e:
    #         print(e)
    #         conn.rollback()
    #         print('修改失败')
    # else:
    #     pass
