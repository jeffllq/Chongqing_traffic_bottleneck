##
# 路段号 车道数 路段级别 上游路段 下游路段
# 已知路段的速度，目的是用路段的瞬时速度（5min），找到发生突变的路段
# 所以，需要的是：路段上下游关系，然后对比上下游速度差#


##
# 已知link的拓扑关系，可以通过link找到对应的roadid，将前者的结点信息赋给后者
# （1）对于单向的link，可以直接赋值
# （2）对于双向的link，如何分配起始节点的顺序给两个ROADID？
#       解决方法：如果两个link的fromnode相同，则其中双向link的二倍ID的roadid的方向和其相同，另一roadid则相反#

import pandas as pd
import copy
import json

#递归调用该构建拓扑的函数，指导找到全部roadid的起始node
def construct_topo(df_temp_DXD_0, df_temp_DXD_1):
    df_temp_DXD_0_copy = copy.deepcopy(df_temp_DXD_0) #深拷贝
    df_temp_DXD_0_copy['ID'] = df_temp_DXD_0_copy['ID']+0.5 #双向link的某一车道
    df_road_topo = pd.concat([df_temp_DXD_0, df_temp_DXD_0_copy]).reset_index(drop=True)
    df_road_topo['flag'] = 0
    # print(df_road_topo) #需要填充的双向link的信息

    #双向link的情况一：和单向link连接，直接判断2*ID的roadid的road的起始节点和单向link一样
    df_temp = pd.merge(df_temp_DXD_0, df_temp_DXD_1, how='inner', on=['FROMNODENO'])
    # print(df_temp)
    list_temp_ID = list(df_temp['ID_x'].values)
    df_temp['ID_x'] = df_temp['ID_x']+0.5
    list_temp_ID_0 = list(df_temp['ID_x'].values)
    set_temp_ID = set(list_temp_ID) #删除重复元素
    set_temp_ID_0 = set(list_temp_ID_0) #删除重复元素

    df_road_topo['temp_FROMNODENO'] = df_road_topo['FROMNODENO']
    df_road_topo['temp_TONODENO'] = df_road_topo['TONODENO']

    df_road_topo.FROMNODENO[df_road_topo.ID.isin(set_temp_ID_0)] = df_road_topo.temp_TONODENO
    df_road_topo.TONODENO[df_road_topo.ID.isin(set_temp_ID_0)] = df_road_topo.temp_FROMNODENO
    df_road_topo.flag[df_road_topo.ID.isin(set_temp_ID)] = 1
    df_road_topo.flag[df_road_topo.ID.isin(set_temp_ID_0)] = 1
    # print(df_road_topo)
    df_road_topo = df_road_topo.drop(['temp_FROMNODENO','temp_TONODENO'],axis=1)
    print(df_road_topo)

    #双向link的情况二：只和双向link连接，那么上一条明确了road方向的双向link的终止节点ENDNODENO就是当前link的2*ID对应的roadi起始节点FROMNODENO


    # #双向车道两种情况：和单向link连接，直接判断2*ID的roadid的road的起始节点和单向link一样
    # #               只和双向link连接，那么上一条双向link的终止节点就是当前link的2*ID对应的roadi起始节点，
    # df_temp_same_FROMNODENO = pd.merge(df_temp_DXD_0, df_temp_DXD_1, how='inner', on=['FROMNODENO'])
    # print('找到了同样起始点的link\n',df_temp_same_FROMNODENO) #不是所有的双向车道都能找到同样起始节点的单向车道

    return


if __name__ == '__main__':
    df_link = pd.read_csv('data/路网拓扑/link.csv')
    df_road = pd.read_csv('data/路网数据/路网.csv')

    # print(df_link)
    # print(df_road)

    df_temp_DXD_0 = df_link[df_link['DXD']==0] #双向的link
    df_temp_DXD_1 = df_link[df_link['DXD']==1] #单向的link
    # print("双向的link\n",df_temp_DXD_0)
    # print("单向的link\n",df_temp_DXD_1)
    df_link_topo = construct_topo(df_temp_DXD_0, df_temp_DXD_1)
