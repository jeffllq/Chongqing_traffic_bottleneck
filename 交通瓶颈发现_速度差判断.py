import pandas as pd
import pymysql

#车速原始数据字段
# columns = ['时间', '路段编号', '计算车辆样本数', '里程覆盖率', '车速',
#            '路段长度', '低速行驶里程', '低速行驶里程占比', '行驶时间',
#            '低速行驶时间', '通过路段时间（预估）', '数据源', '融合标记']
# 数据库里对应:
# time road_id count miles_coverage speed road_length lowspeed_miles
# lowspeed_miles_coverage running_time ls_time ls_coverage road_running_time source flag

Threshold_congestion_speed = 10 #速度差阈值10km/h

#判断是否拥堵，这里依据原专利的中度拥堵的阈值
def is_congestion(road_id, speed, time):
    res = df_road_info[df_road_info['ROADID']==road_id]
    print(res)
    tmp = res.ROADLEVEL.values[0]
    switch = {
        "主干路": lambda  x:(x<=20),
        "次干路": lambda  x:(x<=15),
        "快速路": lambda  x:(x<=35),
        "匝道": lambda  x:(x<=15),
        "支路": lambda  x:(x<=15)
    }
    try:
        is_congestion_flag = switch[tmp](int(speed))
        print("是否拥堵",is_congestion_flag)
    except KeyError as e:
        print("速度判断失败！")
        pass
    return True

def get_road_info():
    df = pd.read_csv('data/路网数据/路网.csv', header=0)
    return df



if __name__ == '__main__':
    global df_road_info
    df_road_info = get_road_info()
    is_congestion(9325,10,202010)
    # db = pymysql.connect(host='172.20.53.155',user='cqu',passwd='cqu1514',db='重庆交通规划研究院')
    # cursor = db.cursor()
    # SQL = "SELECT * FROM 202110speed where road_id=6352"
    # cursor.execute(SQL)
    # res = cursor.fetchone()
    #
    # print(res )
