from datetime import datetime, timedelta
import pandas as pd
from DBconnection import DB

#车速原始数据字段
# columns = ['时间', '路段编号', '计算车辆样本数', '里程覆盖率', '车速',
#            '路段长度', '低速行驶里程', '低速行驶里程占比', '行驶时间',
#            '低速行驶时间', '通过路段时间（预估）', '数据源', '融合标记']
# 数据库里对应:
# time road_id count miles_coverage speed road_length lowspeed_miles
# lowspeed_miles_coverage running_time ls_time ls_coverage road_running_time source flag

Threshold_congestion_speed = 10 #速度差阈值10km/h
Threshold_speed_difference = 0.75 #速度差生效频率阈值 人为设置

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
    df1 = pd.read_csv('data/路网数据/路网.csv', header=0)
    df2 = pd.read_csv('data/ROAD上下游关系.CSV', header=0)
    return df1, df2

def get_speed(road_id, T):
    sql = "SELECT * FROM 202110speed WHERE timestamp(time)=\'"+T+"\' AND road_id="+str(road_id)
    result = db.query_db(sql)
    if len(result)==0:
        print("路段",road_id,"在时间",T,"没有速度的记录")
        return None
    # print("查询速度结果：",result)
    print("查询速度结果：",result[0][5])
    speed = result[0][5]
    return float(speed)

def get_speed_difference(road_id, indirect_road, T):
    v_1 = get_speed(road_id, T)
    v_2 = get_speed(indirect_road, T)
    if ((v_1==None) | (v_2==None)):
        return False
    if v_2-v_1>Threshold_speed_difference: #下游路段比当前路段速度快，差值超过阈值
        return True
    return False

#把字符串转成datetime
def string_toDatetime(string):
    return datetime.strptime(string, "%Y-%m-%d %H:%M:%S")

# 加一分钟或5分钟
def datetime_add(T, minutes):
    delta = timedelta(minutes=minutes)
    new_T = string_toDatetime(T) + delta
    return new_T.strftime("%Y-%m-%d %H:%M:%S")

def speed_difference_process(road_id,T,downstream_roadid_list): #速度差处理
    #速度差大于阈值 有多个间接邻接的路段，计算和当前路段的速度差，暂时假设有一个速度差达到阈值，就算做拥堵瓶颈
    count = 0 #T-2 到 T+2的生效频率
    l = len(downstream_roadid_list[1])
    for indirect_road in downstream_roadid_list[1]:
        if get_speed_difference(road_id, indirect_road, T):
            print("满足简单的速度差阈值条件")
            return True
        else:
            if(get_speed_difference(road_id, indirect_road, datetime_add(T,-5))): count+=1
            if(get_speed_difference(road_id, indirect_road, datetime_add(T,-10))): count+=1
            if(get_speed_difference(road_id, indirect_road, datetime_add(T,+5))): count+=1
            if(get_speed_difference(road_id, indirect_road, datetime_add(T,+10))): count+=1
    print("生效频次：", count)
    if count/len(downstream_roadid_list[1])>Threshold_speed_difference:
        return True
    else:
        return False

def find_road_downstream(road_id):
    road_id = 9325
    result_list = []
    result1 = df_road_topo[df_road_topo['上游ROADID']==road_id]
    tmp_list1 = result1['当前ROADID'].values
    tmp_set1 = set(tmp_list1)
    result_list.append(list(tmp_set1))  # 直接相邻的路段
    result2 = df_road_topo[df_road_topo['上游ROADID'].isin(tmp_set1)]
    tmp_list2 = result2['当前ROADID'].values
    result_list.append(list(set(tmp_list2))) # 间接相邻的路段
    # print(result_list) # [[直接相邻roadid], [简介相邻roadid]]
    return result_list

def is_traffic_bottleneck(road_id, T): #指定路段和时间片，判断该路段是否为交通瓶颈
    V_d = 0 #速度差
    downstream_roadid_list = find_road_downstream(road_id)#找到road_id的邻接路段
    print("邻接的路段：",downstream_roadid_list)
    flag_traffic_bottleneck = speed_difference_process(road_id,T,downstream_roadid_list)
    return flag_traffic_bottleneck

if __name__ == '__main__':
    global df_road_info
    global df_road_topo
    df_road_info, df_road_topo = get_road_info()
    global db
    db = DB()
    while(1):
        print("----------------------------------------------------------------------")
        road_id = input("请输入查询路段id：")
        if road_id=='exit':
            print("结束查询")
            break
        T = input("请输入查询时间（英文格式2021-10-01 12:40:00）：")
        result = is_traffic_bottleneck(road_id, T)
        print(road_id,"是否瓶颈的判断结果：", result)
        print("----------------------------------------------------------------------\n")


    # db = pymysql.connect(host='172.20.53.155',user='cqu',passwd='cqu1514',db='重庆交通规划研究院')
    # cursor = db.cursor()
    # SQL = "SELECT * FROM 202110speed where road_id=6352"
    # cursor.execute(SQL)
    # res = cursor.fetchone()
    #
    # print(res )
