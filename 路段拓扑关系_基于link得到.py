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
import warnings
import json

warnings.filterwarnings('ignore')



def get_direction(df_temp_DXD_0, df_temp_DXD_1, df_road_topo):
    #双向link的情况一：和单向link连接，直接判断2*ID的roadid的road的起始节点和单向link一样
    df_temp = pd.merge(df_temp_DXD_0, df_temp_DXD_1, how='inner', on=['FROMNODENO'])
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

    #更新单向link
    df_temp_new = df_temp[['ID_x', 'FROMNODENO', 'TONODENO_x']].rename(
        columns={'ID_x': 'ID', 'FROMNODENO': 'TONODENO', 'TONODENO_x': 'FROMNODENO'})  # 掉换起始节点
    # print(df_temp_new)
    df_temp_DXD_1 = pd.concat([df_temp_DXD_1, df_temp_new], axis=0).drop_duplicates(subset=['ID'],keep='first').reset_index(drop=True)
    df_temp_DXD_0 = df_temp_DXD_0.drop(df_temp_DXD_0[(df_temp_DXD_0['ID'].isin(set_temp_ID_0)) | (df_temp_DXD_0['ID'].isin(set_temp_ID))].index).reset_index(drop=True)
    # print('新的单向link', df_temp_DXD_1)
    return df_temp_DXD_0,df_temp_DXD_1,df_road_topo

#调用该构建拓扑的函数，指导找到全部roadid的起始node
def construct_topo(df_temp_DXD_0, df_temp_DXD_1):
    df_temp_DXD_0_copy = copy.deepcopy(df_temp_DXD_0) #深拷贝
    df_temp_DXD_0_copy['ID'] = df_temp_DXD_0_copy['ID']+0.5 #双向link的某一车道
    df_road_topo = pd.concat([df_temp_DXD_0, df_temp_DXD_0_copy]).reset_index(drop=True)
    df_road_topo['flag'] = 0 #需要填充的双向link的信息

    tmp = 0
    #双向link的情况二：只和双向link连接，那么上一条明确了road方向的双向link的终止节点ENDNODENO
    # 就是当前link的2*ID对应的roadi起始节点FROMNODENO;上一个双向link的终点作为新的单向link的起点
    while(1):
        print('计数flag',(df_road_topo['flag']==0).sum())
        if ((df_road_topo['flag']==0).sum()>0):
            df_temp_DXD_0,df_temp_DXD_1,df_road_topo = get_direction(df_temp_DXD_0, df_temp_DXD_1, df_road_topo)
            print(df_temp_DXD_0.shape[0])
            print(df_temp_DXD_0)
            if (df_temp_DXD_0.shape[0]==tmp):
                df_temp_DXD_0.to_csv('data/temp_孤立的双向link.csv', index=False)
                print("清空双向link");break
            # print('临时打印',df_road_topo)
            # print('临时打印',df_temp_DXD_1)
            tmp = df_temp_DXD_0.shape[0]
            continue
        else:
            print('找到完整拓扑')
            break
    print('____________________________________________________________________')
    print('找到完整的topo结构如下\n',df_road_topo)
    print('____________________________________________________________________')
    return df_road_topo


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
    df_link_topo.to_csv('data/temp_road_topo.csv', index=False)
