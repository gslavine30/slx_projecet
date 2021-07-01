import pymysql
import jieba
import re

conn = pymysql.connect(host="101.201.125.180", user="root", password="shuinori", db='enterprise_data', port=3306,
                       charset='utf8')
cursor = conn.cursor()


def get_scope(nums=10):
    """
    获取到企业经营范围
    :return:
    """
    sql = """
        select id,scope from ep_base_info limit {}
    """.format(nums)
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def get_industry_by_keyword(keyword):
    """
    根据关键词检索行业
    :return:
    """
    query_param = ['%%%s%%' % keyword]
    sql = """select code from industry where title like %s"""
    cursor.execute(sql, query_param)
    temp = cursor.fetchall()
    result = []
    for i in temp:
        result.append(i[0][0:2])
    return result


def get_title_by_industry_code(code_list):
    """
    根据行业code列表获取一级和二级行业描述
    :return:
    """
    sql = """select title from industry where code = %s and depth = 1
            union
            select title from industry where code = (select parent_code from industry where code = %s and depth =1)
            """
    cursor.execute(sql, (code_list, code_list))
    result = cursor.fetchall()
    return result


def update_industry_by_scope_root(industry_by_scope_root, industry_by_scope_second, id):
    """
    更新企业的经营范围行业
    :param industry_by_scope_root:
    :param industry_by_scope_second:
    :param id:
    :return:
    """
    sql = """update ep_base_info set industry_by_scope_root=%s,industry_by_scope_second=%s where id = %s"""
    cursor.execute(sql, (industry_by_scope_root, industry_by_scope_second, id))
    conn.commit()


def scope_qingxi(scope):
    scope_text = re.sub(u"\\(.*?\\)|\\{.*?}|\\[.*?]", "", scope)
    scope_text = re.sub(u"\\（.*?）|\\{.*?}|\\[.*?]|\\【.*?】", "", scope_text)
    scope_text = re.sub("[\s+\.\!\/_,$%^*(+\"\')]+|[+——()?【】“”！，：；。？、~@#￥%……&*（）]+", "", scope_text)
    temp = str(scope).replace("从事本企业生产同类产品的商业批发和进出口业务。", ""). \
        replace("检验检测服务", ""). \
        replace("技术开发", "").replace("生产", ""). \
        replace("提供", "").replace("一般项目", ""). \
        replace("销售", "").replace("的研发", "").replace("制造", "").replace("。", ""). \
        replace("(不含危险化学品易制毒化学品及监控化学品)", "").replace("(上述研发限下属公司经营)", "").replace("的进出口业务", ""). \
        replace("(依法须经批准的项目经相关部门批准后方可开展经营活动具体经营项目以审批结果为准)", ""). \
        replace("及", "").replace("技术推广", "").replace("设计", "").replace("制作", ""). \
        replace("代理", "").replace("维护", "").replace("配件", "").replace("相关", ""). \
        replace("各类", "").replace("许可项目","")
        # replace("、", " ").replace("；", " ").replace(",", " ").replace(";", " ").replace(",", " "). \
    # replace("(", " ").replace(")", " ").replace("：", " ")
    return scope_text


if __name__ == "__main__":
    ep_list = []
    ep = get_scope(10)
    for i in ep:
        product_list = []
        scope = i[1]
        scope_text = scope_qingxi(scope)
        print(scope_text)
        jieba.load_userdict("./dict.txt")
        seg_list = jieba.cut(scope_text, cut_all=True)
        for i in seg_list:
            product_list.append(i)
        ep_list.append(product_list)

    for i in ep_list:
        print(i)
