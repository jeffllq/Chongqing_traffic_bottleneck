import pandas as pd
import os

#最终结果 时间，路段，是否为瓶颈
def transition(df):
    result_list = []
    T_list = df.time.values
    df.set_index(['time'], inplace=True)
    # print(T_list)
    cols = df.columns
    for road_id in cols[0:]:
        for T in T_list:
            if df.loc[T, road_id] == True:
                # tmp = [T, road_id, True]
                result_list.append([T, road_id, '1'])
    if os.path.exists('data/output/周末（10月19日周六）瓶颈查询结果.txt'):
        os.remove('data/output/周末（10月19日周六）瓶颈查询结果.txt')
    file_write_obj = open('data/output/周末（10月19日周六）瓶颈查询结果.txt', 'w')
    for line in result_list:
        line = ','.join(line)
        file_write_obj.writelines(line)
        file_write_obj.write('\n')
    file_write_obj.close()



if __name__ == '__main__':
    df_workday = pd.read_csv('data/Result_bottleneck/工作日（10月16日周三）瓶颈查询结果.csv', header=0)
    df2_weekend = pd.read_csv('data/Result_bottleneck/周末（10月19日周六）瓶颈查询结果.csv', header=0)

    # transition(df_workday)
    transition(df2_weekend)



