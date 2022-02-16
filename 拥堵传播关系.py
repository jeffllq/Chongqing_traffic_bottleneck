##
# 本文件的目的在于找到路段间拥堵传播的因果关系
# 需要两个处理的前提：1、路段间的拥堵关联；2、生成树找出拥堵传播因果关系（可解释性不够）；3、拥堵传播概率。
# 拥堵关联性：空间关系（由于没有经纬度信息，改为直接连接｜间接相连）、时间关系（下游路段拥堵的时间滞后）#

import pandas as pd
from DBconnection import DB
import numpy as np
import copy
from datetime import datetime, timedelta
import json
from treelib import Tree


#判断是否拥堵，这里依据原专利的中度拥堵的阈值
def is_congestion(road_id, speed, time):
    res = df_road_info[df_road_info['ROADID']==road_id]
    # print(res)
    tmp = res.ROADLEVEL.values[0]
    # print("道路级别",tmp)
    switch = {
        "主干路": lambda  x:(x<=20),
        "次干路": lambda  x:(x<=15),
        "快速路": lambda  x:(x<=35),
        "匝道": lambda  x:(x<=15),
        "支路": lambda  x:(x<=15)
    }
    try:
        if speed==None: return False
        is_congestion_flag = switch[tmp](int(speed))
        # print("是否拥堵",is_congestion_flag)
    except KeyError as e:
        # print("拥堵判断失败！")
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

def get_sample_speed(T_start, T_end):
    sql = "SELECT speed,road_id,time FROM 202110speed WHERE timestamp(time)>=\'"+T_start+"\' and timestamp(time)<=\'"+T_end+"\'"
    print(sql)
    result = db.SELECT(sql)
    # print(result)
    if len(result)==0:
        print("没有找到指定时间段内（",T_start, T_end,"）的速度记录！")
        return None
    # print("查询速度结果：",result)
    # print("查询速度结果：",result[0][0])
    df_result = pd.DataFrame(result, columns=['speed','road_id','time'])
    # print(df_result)
    df_result.to_csv('tmp_sample_speed.csv', index=False)
    return df_result

def get_road_info():
    df1 = pd.read_csv('data/路网数据/路网.csv', header=0)
    df2 = pd.read_csv('data/ROAD上下游关系.CSV', header=0)
    return df1, df2

def temporal_relationship_congestion(dict_spatio, df_sample_speed):
    #将空间关联性结果传入，只考察空间相关的路段是否存在时间关联
    #时间关联的结果跟选取的观测时间段有关
    #使用工作日中8到20点的时间段作为观测时间段
    dict_temoral = {}
    print(df_sample_speed)
    print("总共考察路段数量：",len(dict_spatio.keys()))
    count = 0
    for key in dict_spatio.keys():
        # '2021-10-16 08:00:00', '2021-10-16 20:00:00'
        print("考察第",count,"个路段:",key)
        count+=1
        T = '2021-10-16 08:00:00'
        tmp_list = []
        print("邻接的路段：", dict_spatio.get(key))
        neighbor_road_list = dict_spatio.get(key)
        while(string_toDatetime(T)<=string_toDatetime('2021-10-16 20:00:00')):
            try:
                speed_key = df_sample_speed[(df_sample_speed['road_id'] ==key)  & (df_sample_speed['time'] == T)]['speed'].values[0]
            except:
                T = datetime_add(T, 5)
                # print("更新时间", T)
                continue
            if(is_congestion(key, speed_key, T)):
                for road_id in neighbor_road_list:
                    try:
                        speed_neighbor = df_sample_speed[(df_sample_speed['road_id'] ==road_id)  & (df_sample_speed['time'] == T)]['speed'].values[0]
                    except:
                        # print("没找到跳过")
                        continue
                    if(is_congestion(road_id, speed_neighbor, T)):
                        continue
                    try:
                        speed_neighbor = df_sample_speed[(df_sample_speed['road_id'] == road_id) & (df_sample_speed['time'] == datetime_add(T,5))]['speed'].values[0]
                    except:
                        # print("没找到跳过")
                        continue
                    if(is_congestion(road_id, speed_neighbor, datetime_add(T,5))):
                        tmp_list.append(road_id)
                        print("拥堵时间相连关系找到！")
            T = datetime_add(T, 5)
            # print("更新时间", T)
        dict_temoral[key] = tmp_list
    print(dict_temoral)
    with open('中间数据/拥堵关联关系.txt','w') as file:
        file.write(json.dumps(dict_temoral))
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
        tmp_list = list(set(tmp_list))
        dict_spatio[key] = tmp_list
    print(dict_spatio)

    # df_sample_speed = get_sample_speed('2021-10-16 8:00:00', '2021-10-16 12:00:00')
    df_sample_speed = pd.read_csv('tmp_sample_speed.csv', header=0)
    #时间关系：下游发生拥堵的时间为上有路段拥堵的时间滞后，应该为一个间隔
    df_temporal = pd.DataFrame(columns=road_id_list, index=road_id_list).fillna(0)
    df_temporal = temporal_relationship_congestion(dict_spatio, df_sample_speed)
    df_congestion_related = copy.deepcopy(df_temporal)
    return

def spanning_tree(congestion_correlation):
    #假设传入的是（父节点，子节点）的有向关系
    Trees = set()
    for correlation in congestion_correlation:
        print()
        Tree.cre

    return

def congestion_propagation_causal():
    congestion_related()
    congestion_correlation = open('拥堵关联关系.txt','r')
    #对congestion_correlation格式转换为字典
    spanning_tree(congestion_correlation)
    return

if __name__ == '__main__':
    global db
    db = DB()
    global df_road_info, df_road_topo
    df_road_info, df_road_topo = get_road_info()
    congestion_propagation_causal()