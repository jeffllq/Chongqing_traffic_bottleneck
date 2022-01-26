##
# 路段号 车道数 路段级别 上游路段 下游路段
# 已知路段的速度，目的是用路段的瞬时速度（5min），找到发生突变的路段
# 所以，需要的是：路段上下游关系，然后对比上下游速度差#


##
# 已知link的拓扑关系，可以通过link找到对应的roadid，将前者的结点信息赋给后者
# （1）对于单向的link，可以直接赋值
# （2）对于双向的link，如何分配起始节点的顺序给两个ROADID？#

import pandas as pd
import json


if __name__ == '__main__':
    df_link = pd.read_csv('data/路网拓扑/link.csv')
    df_road = pd.read_csv('data/路网数据/路网.csv')
    print(df_link)
    print(df_road)

