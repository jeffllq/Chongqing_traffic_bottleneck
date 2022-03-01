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
import networkx as nx
import matplotlib.pyplot as plt


#判断是否拥堵，这里依据原专利的中度拥堵的阈值
def is_congestion(road_id, speed):
    res = df_road_info[df_road_info['ROADID']==road_id]
    try:
        tmp = res.ROADLEVEL.values[0]
    except:
        tmp = '支路' #如果没有路段级别信息，默认为支路
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
        return is_congestion_flag
        # print("是否拥堵",is_congestion_flag)
    except KeyError as e:
        # print("拥堵判断失败！")
        return False

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
    df_road_info = pd.read_csv('data/路网数据/路网.csv', header=0)
    df_road_topo = pd.read_csv('data/ROAD上下游关系.csv', header=0)
    return df_road_info, df_road_topo

def temporal_relationship_congestion(dict_spatio, df_sample_speed):
    #将空间关联性结果传入，只考察空间相关的路段是否存在时间关联
    #时间关联的结果跟选取的观测时间段有关
    #使用工作日16点到20点的时间段作为观测时间段
    dict_temoral = {}
    result_list = []

    # print("总共考察路段数量：",len(dict_spatio.keys()))
    count = 0
    for key in dict_spatio.keys():
        print("考察第",count,"个路段:",key)
        count+=1
        T = '2021-10-19 16:00:00'
        tmp_list = []
        # print("邻接的路段：", dict_spatio.get(key))
        neighbor_road_list = dict_spatio.get(key)
        while(string_toDatetime(T)<=string_toDatetime('2021-10-19 20:00:00')):
            try:
                speed_key = df_sample_speed[(df_sample_speed['road_id'] ==key)
                                            & (df_sample_speed['time'] == T)]['speed'].values[0] #查看当前路段速度
            except:
                T = datetime_add(T, 5) # print("更新时间", T)
                continue
            if(is_congestion(key, speed_key)): #当前路段拥堵，再查看下游路段是否拥堵
                for road_id in neighbor_road_list:
                    try:
                        speed_neighbor = df_sample_speed[(df_sample_speed['road_id'] ==road_id)  &
                                                         (df_sample_speed['time'] == T)]['speed'].values[0]
                    except:
                        continue
                    if(is_congestion(road_id, speed_neighbor)): #如下下游路段在同一个时间片拥堵，不具备时间滞后性
                        print("跳过")
                        continue
                    # for t in (datetime_add(T, 5), datetime_add(T, 10),datetime_add(T, 15)):
                    for t in ( datetime_add(T, 10),datetime_add(T, 15)):
                        try:
                            speed_neighbor = df_sample_speed[(df_sample_speed['road_id'] == road_id) &
                                                             (df_sample_speed['time'] == t)]['speed'].values[0]
                        except:
                            continue
                        if(is_congestion(road_id, speed_neighbor)):
                            tmp_list.append(road_id)
                            result_list.append((T, key, road_id))
                            #保存当前路段在哪个时间片跟下游哪个路段具有拥堵关联性
                            print("拥堵时间相连关系找到！")
            T = datetime_add(T, 5)
            # print("更新时间", T)
        dict_temoral[key] = list(set(tmp_list))
    # print(dict_temoral)
    return result_list

#发现拥堵传播的关联性
def congestion_related():
    #空间关系：直接相连(暂时不包含间接相连)
    df_tmp = df_road_topo[['当前ROADID','下游ROADID']].drop_duplicates().reset_index(drop=True)
    print(df_tmp)
    road_id_list = list(set(df_tmp['当前ROADID'].values.tolist()))
    dict_spatio = {}
    for road_id in road_id_list:
        df = df_tmp[df_tmp['当前ROADID']==road_id]
        tmp_list = list(set(df['下游ROADID']))
        dict_spatio[road_id] = tmp_list
    print(dict_spatio) #找到空间上直接相邻的关系，存储为字典，路段：[相邻路段]

    # df_sample_speed = get_sample_speed('2021-10-19 16:00:00', '2021-10-19 20:00:00') #读取样本速度文件到内存，减少程序执行时间
    df_sample_speed = pd.read_csv('tmp_sample_speed.csv', header=0)
    #时间关系：下游发生拥堵的时间为上有路段拥堵的时间滞后，设置为1到3个时间片间隔
    result_spatio_temporal = temporal_relationship_congestion(dict_spatio, df_sample_speed)
    df = pd.DataFrame(result_spatio_temporal, columns=['time', 'congestion_from_road', 'congestion_to_road']).drop_duplicates()

    df.to_csv('中间数据/拥堵关联关系(10到15分钟).csv', index=False)
    # with open('中间数据/拥堵关联关系(10到15分钟).txt','w') as file:
    #     file.write(json.dumps(dict_temoral))
    return

def congestion_occurrence_probability():
    # 计算拥堵发生概率（一）：定义为周一到周五，5个工作日内(10月的18-22号)，同一时间段发生拥堵的概率
    # 计算拥堵发生概率（二）：定义工作日内(10月的19号)，晚上16到20点，拥堵时间占总时长的比例
    # df_sample_speed = pd.read_csv('tmp_sample_speed.csv', header=0)
    # df_sample_speed['congestion_flag'] = 0
    # print(df_sample_speed.shape[0])
    # for idx in range(df_sample_speed.shape[0]):
    #     speed = df_sample_speed.loc[idx, 'speed']
    #     road_id = df_sample_speed.loc[idx, 'road_id']
    #     if is_congestion(road_id, speed):
    #         df_sample_speed.loc[idx, 'congestion_flag'] = 1
    # df_sample_speed.to_csv('中间数据/带拥堵判断结果的速度记录.csv', index=False)

    Total_duration = 48 #总共48个时间片
    df_sample_speed = pd.read_csv('中间数据/带拥堵判断结果的速度记录.csv', header=0)
    road_id_list = list(set(df_sample_speed['road_id'].values.tolist()))
    dict_res = {}
    for road_id in road_id_list:
        df_tmp = df_sample_speed[df_sample_speed['road_id']==road_id]
        # print("拥堵发生次数",df_tmp['congestion_flag'].sum())
        ratio = round(df_tmp['congestion_flag'].sum()/Total_duration, 2)
        print(road_id,"的拥堵发生概率",ratio)
        dict_res[road_id] = ratio
    with open('中间数据/路段拥堵发生概率.txt','w') as file:
        file.write(json.dumps(dict_res))
    return

#构建拥堵关联关系的树，这里是怎么结合时间标签的？？？？？？？？？？？？？？？？？？？？？？
def create_road_correlation_tree(df_corr):
    # print("拥堵的时空关联性",df_corr)
    corr_list = df_corr.values.tolist()
    corr_set = set()
    for item in corr_list:
        item = (item[0], item[1], item[2])
        corr_set.add(item)

    #如何考虑时间标签 暂时不考虑

    CPG = list()
    G = nx.DiGraph()
    CPG.append(G)

    while(len(corr_set)>0):
        corr_pair = corr_set.pop() #随机选一个关系
        road_1 = corr_pair[1]
        road_2 = corr_pair[2]
        m = -1
        n = -1 #初始编号

        for j in range(0, len(CPG)): #找第一个路段存在的图
            if road_1 in CPG[j]:
                m = j
        for j in range(0, len(CPG)): #找第二个路段存在的图
            if road_2 in CPG[j]:
                n = j

        #1、存在于同一个图中
        if((m==n)&(m>=0)):
            if CPG[m].has_edge(road_1, road_2): #不存在连接的有向边，则添加有向边（这里先不做环的处理）
                continue
            else:
                CPG[m].add_edge(road_1, road_2)
        #2、两个路段存在于不同的图中
        if((m>=0)&(n>=0)&(m!=n)):
            CPG[m] = nx.compose(CPG[m],CPG[n])#合并两个图，并不会有画有向边的操作
            CPG[m].add_edge(road_1, road_2) #画出新的有向边
            CPG.remove(CPG[n])
        #3、一个路段在某个图中，另一路段不在任何一个图中
        if((m>=0)&(n==-1)):
            CPG[m].add_edge(road_1, road_2)
        elif((m==-1)&(n>=0)):
            CPG[n].add_edge(road_1, road_2)
        #4、两个路段均不在任何一个图中
        if((m==-1)&(n==-1)):
            G_new = nx.DiGraph()
            G_new.add_edge(road_1, road_2)
            CPG.append(G_new)
            # print(len(CPG))

    print(len(CPG)) #最后得到37个拥堵传播图
    # plt.figure()
    # for G in CPG:
    #     nx.draw(G, with_labels=True)
    #     plt.show()
    return CPG #返回拥堵传播图的集合


def congestion_propagation_causal():
    # congestion_related() #发现路段间拥堵的关联关系，写入中间文件
    df_corr = pd.read_csv('中间数据/拥堵关联关系(10到15分钟).csv',header=0)
    CPG = create_road_correlation_tree(df_corr) #构建拥堵传播因果关系图，返回图的集合


    return

if __name__ == '__main__':
    global db
    db = DB()
    global df_road_info, df_road_topo
    df_road_info, df_road_topo = get_road_info()
    congestion_propagation_causal()