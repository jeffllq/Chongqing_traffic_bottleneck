import pymysql
import time
class DB:
    def __init__(self):
        try:
            self.db_con = pymysql.connect(host='172.20.53.155', user='cqu', passwd='cqu1514', db='重庆交通规划研究院')
        except Exception as e:
            print("数据库连接失败：%s"%e)

    def close(self):
        self.db_con.close()

    def query_db(self, query):
        print("本次执行的查询query语句：%s" %query)
        try:
            cursor = self.db_con.cursor()
            result = cursor.execute(query)
            return result
        except Exception as e:
            print("数据库查询失败：%s" % e)
            cursor.close()
            exit()

    def alter_db(self, query):
        print("本次执行的删改query语句：%s" %query)
        try:
            cursor = self.db_con.cursor()
            cursor.execute(query)
            self.db_con.commit()
            return True
        except Exception as e:
            print("数据库删改失败：%s" % e)
            cursor.close()
            exit()