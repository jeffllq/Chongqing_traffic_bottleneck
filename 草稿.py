from treelib import Tree
import pandas as pd
from copy import deepcopy

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
    while(new_tree.contains(tmp_tree.root)):
        tr

    for tree1 in tree_list:
        for tree2 in tree_list:
            if tree1.contains(tree2.root):
                print("jhhh")
                tree1.paste(tree2.root, tree2)
                tree1.show()




def create_propagation_probability():
    return

if __name__ == '__main__':
    create_road_correlation_tree()





