##
# 路段号 车道数 路段级别 上游路段 下游路段
# 已知路段的速度，目的是用路段的瞬时速度（5min），找到发生突变的路段
# 所以，需要的是：路段上下游关系，然后对比上下游速度差#

import pandas as pd
import json



#判断两个路段是否连接
def is_connected(road_1, road_2):
    flag = False
    return flag

def position_transform(transform,position):


    return 1

if __name__ == '__main__':
    f = open('data/路网数据/路网.json')
    topojson_sample = json.load(f)
    # print(topojson_sample.keys()) #'type', 'arcs', 'transform', 'objects'
    arcs = topojson_sample['arcs'] #路段的弧线表示
    transform = topojson_sample['transform'] #转换基准
    objects = topojson_sample['objects'] #路段
    road_info = {}
    for road_geometry in objects['路网']['geometries']:
        arcs_index = road_geometry['arcs'][0] #对应弧线段
        print(arcs_index)
        print(arcs[arcs_index])
        CDS = road_geometry['properties']['CDS'] #对应的路段的基本信息
        ROADLEVEL = road_geometry['properties']['ROADLEVEL'] #对应的路段的基本信息
        ROADID = road_geometry['properties']['ROADID'] #对应的路段的基本信息
        # temp_info = {'CDS':CDS, 'ROADLEVEL':ROADLEVEL, 'ROADID':ROADID, 'arc':arcs[arcs_index], 'Upstream':None,'Downstream':None}
        # road_info[ROADID] = temp_info #将路段信息重新整理到road_info

    # print(road_info)

    # 检查是否重复roadid
    # l = list(road_info.keys())
    # s = set(l)
    # if len(l)==len(s):
    #     print(True)








