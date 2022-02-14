##
# 本文件的目的在于找到路段间拥堵传播的因果关系
# 需要两个处理的前提：1、路段间的拥堵关联；2、生成树找出拥堵传播因果关系（可解释性不够）；3、拥堵传播概率。
# 拥堵关联性：空间关系（由于没有经纬度信息，改为直接连接｜间接相连）、时间关系（下游路段拥堵的时间滞后）#

import pandas as pd
from DBconnection import DB
import numpy as np
import copy
from datetime import datetime, timedelta


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

#把字符串转成datetime
def string_toDatetime(string):
    return datetime.strptime(string, "%Y-%m-%d %H:%M:%S")

# 加一分钟或5分钟
def datetime_add(T, minutes):
    delta = timedelta(minutes=minutes)
    new_T = string_toDatetime(T) + delta
    return new_T.strftime("%Y-%m-%d %H:%M:%S")

def get_speed(road_id, T):
    sql = "SELECT speed FROM 202110speed WHERE timestamp(time)=\'"+T+"\' AND road_id="+str(road_id)
    result = db.SELECT(sql)
    # print(result)
    if len(result)==0:
        # print("路段",road_id,"在时间",T,"没有速度的记录")
        return None
    # print("查询速度结果：",result)
    # print("查询速度结果：",result[0][0])
    speed = result[0][0]
    return float(speed)

def get_road_info():
    df1 = pd.read_csv('data/路网数据/路网.csv', header=0)
    df2 = pd.read_csv('data/ROAD上下游关系.CSV', header=0)
    return df1, df2

def temporal_relationship_congestion(dict_spatio):
    #将空间关联性结果传入，只考察空间相关的路段是否存在时间关联
    #时间关联的结果跟选取的观测时间段有关
    #假设就使用工作日中8到20点的时间段作为观测时间段
    dict_temoral = {}
    for key in dict_spatio:
        # '2021-10-16 8:00:00', '2021-10-16 20:00:00'
        T = '2021-10-16 8:00:00'
        while(T<='2021-10-16 20:00:00'):
            if(is_congestion(key, get_speed(key, T), T)):


        return

def congestion_related():
    road_id_list = df_road_info.ROADID.values.tolist()
    #空间关系：直接相连(暂时不包含间接相连)
    # df_spatio = pd.DataFrame(columns=road_id_list, index=road_id_list).fillna(0)
    # print(df_spatio)
    # for topo in df_road_topo.values.tolist():
    #     road1 = topo[1]
    #     road2 = topo[2]
    #     if ((np.isnan(road1)) or (np.isnan(road2))): continue
    #     if ((road1 not in road_id_list) or (road2 not in road_id_list)): continue
    #     df_spatio.loc[int(road1), int(road2)] = 1
    # # print(df_spatio)
    spatio_groups = df_road_topo.groupby(['当前ROADID']).groups
    dict_spatio = {}
    for key in spatio_groups:
        index_list = spatio_groups.get(key).tolist()
        tmp_list = []
        for i in index_list:
            downstream_road_id = df_road_topo.loc[i, '下游ROADID']
            tmp_list.append(downstream_road_id)
        dict_spatio[key] = tmp_list
    # print(dict_spatio)

    #时间关系：下游发生拥堵的时间为上有路段拥堵的时间滞后，应该为一个间隔
    df_temporal = pd.DataFrame(columns=road_id_list, index=road_id_list).fillna(0)
    df_temporal = temporal_relationship_congestion(dict_spatio)
    df_congestion_related = copy.deepcopy(df_temporal)
    return

def congestion_propagation_causal():
    congestion_related()
    return

if __name__ == '__main__':
    global db
    db = DB()
    global df_road_info, df_road_topo
    df_road_info, df_road_topo = get_road_info()
    congestion_propagation_causal()