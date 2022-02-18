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

def create_road_correlation_tree():
    df_road_topo = pd.read_csv('data/ROAD上下游关系.CSV', header=0)
    df_road_topo = df_road_topo[['当前ROADID','下游ROADID']].drop_duplicates().reset_index(drop=True)
    # tmp_list = list(set(df_road_topo.values.tolist()))
    road_topo_groups = df_road_topo.groupby(['当前ROADID']).groups
    tree_list = []
    for key in road_topo_groups.keys():
        tree = Tree()
        tree.create_node(key, key)
        for value in road_topo_groups.get(key):
            road_id = df_road_topo.loc[value,'下游ROADID']
            tree.create_node(road_id,road_id,parent=key)
        # tree.show()
        tree_list.append(tree)

    new_tree = tree_list[0]
    tmp_tree = tree_list[1]

def spatial_temporal_threshold():
    # 两个路段的最短路径距离小于空间阈值T_s
    # 拥堵发生滞后时间小于时间阈值T_t
    return

def congestion_occurrence_probability():
    # 计算拥堵发生概率（一）：定义为周一到周五，5个工作日内(10月的18-22号)，同一时间段发生拥堵的概率
    # 计算拥堵发生概率（二）：定义工作日内(10月的19号)，晚上16到20点，拥堵时间占总时长的比例
    # df_sample_speed = pd.read_csv('tmp_sample_speed.csv', header=0)
    # df_sample_speed['congestion_flag'] = 0
    Total_duration = 48 #总共48个时间片
    # print(df_sample_speed.shape[0])
    # for idx in range(df_sample_speed.shape[0]):
    #     speed = df_sample_speed.loc[idx, 'speed']
    #     road_id = df_sample_speed.loc[idx, 'road_id']
    #     if is_congestion(road_id, speed):
    #         df_sample_speed.loc[idx, 'congestion_flag'] = 1
    # df_sample_speed.to_csv('中间数据/带拥堵判断结果的速度记录.csv', index=False)
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

def congestion_propagation_probability():
    return

def congestion_propation_speed():
    return



if __name__ == '__main__':
    global db, df_road_info
    db = DB()
    df_road_info = pd.read_csv('data/路网数据/路网.csv', header=0)
    congestion_occurrence_probability()




