##
# 路段号 车道数 路段级别 上游路段 下游路段
# 已知路段的速度，目的是用路段的瞬时速度（5min），找到发生突变的路段
# 所以，需要的是：路段上下游关系，然后对比上下游速度差#


##
# 已知link的拓扑关系，可以通过link找到对应的roadid，将前者的结点信息赋给后者
# （1）对于单向的link，可以直接赋值
# （2）对于双向的link，如何分配起始节点的顺序给两个ROADID？
#       经过观察，2倍ID的roadid方向不变，加一的方向相反

import pandas as pd
import copy
import warnings
import json

warnings.filterwarnings('ignore')


#调用该构建拓扑的函数，指导找到全部roadid的起始node
def construct_topo(df_temp_DXD_0, df_temp_DXD_1):
    df_temp_DXD_0_copy = copy.deepcopy(df_temp_DXD_0)  # 深拷贝
    df_temp_DXD_0_copy['ID'] = df_temp_DXD_0_copy['ID'] + 0.5  # 双向link的某一车道
    df_temp_DXD_0_copy['temp'] = df_temp_DXD_0_copy['TONODENO']
    df_temp_DXD_0_copy['TONODENO'] = df_temp_DXD_0_copy['FROMNODENO']
    df_temp_DXD_0_copy['FROMNODENO'] = df_temp_DXD_0_copy['temp']
    df_temp_DXD_0_copy = df_temp_DXD_0_copy.drop(['temp'], axis=1)
    print(df_temp_DXD_0)
    print(df_temp_DXD_0_copy)
    print(df_temp_DXD_1)
    df_road_topo = pd.concat([df_temp_DXD_0, df_temp_DXD_0_copy,df_temp_DXD_1],axis=0).reset_index(drop=True)
    new_df = df_road_topo[['FROMNODENO','TONODENO','ID','FLAG','TJL','FS','TRAFFIC']]
    print(new_df)
    new_df['ID'] = new_df['ID']*2
    return new_df


if __name__ == '__main__':
    df_link = pd.read_csv('data/路网拓扑/link.csv')
    df_road = pd.read_csv('data/路网数据/路网.csv')

    # print(df_link)
    # print(df_road)

    df_temp_DXD_0 = df_link[df_link['DXD']==0] #双向的link
    df_temp_DXD_1 = df_link[df_link['DXD']==1] #单向的link
    # print("双向的link\n",df_temp_DXD_0)
    # print("单向的link\n",df_temp_DXD_1)
    df_road_topo = construct_topo(df_temp_DXD_0, df_temp_DXD_1)
    df_road_topo.to_csv('data/路网拓扑_测试.csv',index=False)
