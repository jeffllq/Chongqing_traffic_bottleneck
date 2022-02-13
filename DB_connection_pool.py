import pymysql
from DBUtils.PooledDB import PooledDB


class DB_connection_pool():
    host = '172.20.53.155'
    user = 'root'
    port = 3306
    password = 'cqu1514'
    charset = 'utf-8'
    db = '重庆交通规划研究院'
    limit_count = 30
    pool = None

    def __init__(self):
        self.pool = PooledDB(
            pymysql,
            self.limit_count,
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            charset=self.charset,
            use_unicode=True
        )



    # def SELECT(self, sql):
    #     try:
    #         conn = self.pool.connection()
    #         cursor = conn.cursor()
    #         cursor.excute(sql)
    #         result = cursor.fetchall()
    #         cursor.close()
    #         conn.close()
    #         return result
    #     except Exception as e:
    #         print("数据库查询失败：%s" % e)
    #         cursor.close()
    #         exit()
