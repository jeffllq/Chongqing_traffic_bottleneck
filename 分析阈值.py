import json

from treelib import Tree
import pandas as pd
from copy import deepcopy
from DBconnection import DB

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
        if speed==None: return False #如果没有速度记录，默认就不是拥堵
        is_congestion_flag = switch[tmp](int(speed))
        return is_congestion_flag
        # print("是否拥堵",is_congestion_flag)
    except KeyError as e:
        # print("拥堵判断失败！")
        return False

bfgth
|

def spatial_temporal_threshold():
    # 两个路段的最短路径距离小于空间阈值T_s
    # 拥堵发生滞后时间小于时间阈值T_t
    return

def congestion_propagation_probability():
    return

def congestion_propation_speed():
    return

if __name__ == '__main__':
    global db, df_road_info
    db = DB()
    df_road_info = pd.read_csv('data/路网数据/路网.csv', header=0)
    congestion_occurrence_probability()




