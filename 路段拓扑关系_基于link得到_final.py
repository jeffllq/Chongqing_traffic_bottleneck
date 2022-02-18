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
def find_road_nodes(df_temp_DXD_0, df_temp_DXD_1):
    df_temp_DXD_0_copy = copy.deepcopy(df_temp_DXD_0)  # 深拷贝
    df_temp_DXD_0_copy['ID'] = df_temp_DXD_0_copy['ID'] + 0.5  # 双向link的某一车道
    df_temp_DXD_0_copy['temp'] = df_temp_DXD_0_copy['TONODENO']
    df_temp_DXD_0_copy['TONODENO'] = df_temp_DXD_0_copy['FROMNODENO']
    df_temp_DXD_0_copy['FROMNODENO'] = df_temp_DXD_0_copy['temp']
    df_temp_DXD_0_copy = df_temp_DXD_0_copy.drop(['temp'], axis=1)
    print(df_temp_DXD_0)
    print(df_temp_DXD_0_copy)
    print(df_temp_DXD_1)
    df_road_nodes = pd.concat([df_temp_DXD_0, df_temp_DXD_0_copy,df_temp_DXD_1],axis=0).reset_index(drop=True)
    new_df = df_road_nodes[['FROMNODENO','TONODENO','ID','FLAG','TJL','FS','TRAFFIC']]
    print(new_df)
    new_df['ID'] = new_df['ID']*2
    return new_df

def get_road_topo(df_road_nodes, df_road):
    df_road_nodes = df_road_nodes.drop(['FLAG','TJL','FS','TRAFFIC'],axis=1)
    # print('road的起始节点信息\n',df_road_nodes)
    # print('road的信息\n',df_road)
    # df = pd.merge(df_road,df_road_nodes, how='inner',left_on=['ROADID'], right_on=['ID'])
    # print(df)
    df_road_nodes_temp = copy.deepcopy(df_road_nodes)
    df_result = pd.merge(df_road_nodes, df_road_nodes_temp, how='inner', left_on=['TONODENO'],right_on=['FROMNODENO'])
    # print(df_result)
    df_result = df_result.drop(['FROMNODENO_x','TONODENO_x','FROMNODENO_y','TONODENO_y'], axis=1)
    df_downstream = df_result.rename(columns={'ID_x':'当前ROADID', 'ID_y':'下游ROADID'})
    df_upstream = copy.deepcopy(df_downstream).rename(columns={'当前ROADID':'上游ROADID', '下游ROADID':'当前ROADID'})
    df_final = pd.merge(df_upstream, df_downstream, how='outer', on=['当前ROADID'])
    print(df_final)
    for i in range(df_final.shape[0]):
        try:
            if (abs(df_final.loc[i,'下游ROADID']-(df_final.loc[i,'当前ROADID']))==1):
                print("假的上下游")
                df_final = df_final.drop([i])
        except:pass
    df_final.reset_index(drop=True)
    for i in range(df_final.shape[0]):
        try:
            if (abs(df_final.loc[i,'上游ROADID']-(df_final.loc[i,'当前ROADID']))==1):
                print("假的上下游")
                df_final = df_final.drop([i])
        except:pass
    print(df_final)
    df_final.to_csv('data/ROAD上下游关系.CSV', index=False)

if __name__ == '__main__':
    df_link = pd.read_csv('data/路网拓扑/link.csv')
    df_road = pd.read_csv('data/路网数据/路网.csv')

    # print(df_link)
    # print(df_road)

    df_temp_DXD_0 = df_link[df_link['DXD']==0] #双向的link
    df_temp_DXD_1 = df_link[df_link['DXD']==1] #单向的link
    # df_road_nodes = find_road_nodes(df_temp_DXD_0, df_temp_DXD_1)
    # df_road_nodes.to_csv('data/路网拓扑_测试.csv',index=False)
    #将起始点表示的road方向，转换为road间的上下游关系  当前路段roadid 上游roadid 下游roadid
    df_road_nodes = pd.read_csv('data/路网拓扑_节点表示.csv')
    get_road_topo(df_road_nodes, df_road)
