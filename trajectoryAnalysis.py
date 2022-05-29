import math
import numpy as np

import pandas as pd
from pyspark import SQLContext


def dist_ll(lat1, lon1, lat2, lon2):
    # 计算两点间距离 wgs84 单位:m
    # 传入参数为第一点和第二点的纬度和经度
    lat1 = float(lat1)
    lat2 = float(lat2)
    lon1 = float(lon1)
    lon2 = float(lon2)
    R = 6371
    dLat = (lat2 - lat1) * math.pi / 180.0
    dLon = (lon2 - lon1) * math.pi / 180.0

    a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(lat1 * math.pi / 180.0) * math.cos(
        lat2 * math.pi / 180.0) * math.sin(dLon / 2) * math.sin(dLon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    dist = R * c
    return dist * 1000


# 1、把plt文件读成spark dataframe，并计算速度
def calPltSpeed(path):
    pandas_df = pd.read_csv(path, header=None, skiprows=6, usecols=[0, 1, 3, 4, 5, 6],
                            names=['Latitude', 'Longitude', 'Altitude', 'Day_from1899', 'Date',
                                   'Time'])
    # 计算车辆速度，开始和最后都是0
    speed = [0]
    for i in range(pandas_df.shape[0] - 2):
        speed.append(
            dist_ll(pandas_df.loc[i + 1][0], pandas_df.loc[i + 1][1], pandas_df.loc[i][0], pandas_df.loc[i][1]) / (
                    86400 * (float(pandas_df.loc[i + 1][3]) - float(pandas_df.loc[i][3]))))
    speed.append(0)
    pandas_df['speed'] = speed
    # 计算加速度，开始和最后也是0
    acceleration = [0]
    for i in range(pandas_df.shape[0] - 2):
        acceleration.append(pandas_df.loc[i + 1].speed - pandas_df.loc[i].speed / (
                86400 * (float(pandas_df.loc[i + 1][3]) - float(pandas_df.loc[i][3]))))
    acceleration.append(0)
    pandas_df['acceleration'] = acceleration
    pandas_df['id'] = range(1, pandas_df.shape[0] + 1)
    return SQLContext.createDataFrame(pandas_df)



# 2、判断停留点，输入一个dataframe，输出一个含有停留点信息的dataframe
def calc_stop_point(df):
    return df.withColumn('Stop', df.speed < 0.4)



# 3、单调区间分析函数，分析加减速区间并保存成文件
def analyse_speed(points, num, up):
    """
    :param points:输入spark DF的点列表
    :param num:输入最少数目
    :param up:布尔值 true加速 false减速
    :return:void
    """
    # 把停止点表转为list
    p_list = points.collect()
    # 把静止点的序号提取成一个数组
    p_index = []
    # 搞一个区间
    periods = []
    for p_point in p_list:
        p_index.append(p_point.id)

    p_array = np.array(p_index)
    p_group = np.split(p_array, np.where(np.diff(p_array) != 1)[0] + 1)

    # 创建一个空列表
    new_group = []

    # 遍历 arr 中的每个元素
    for element in p_group:
        # 如果元素长度大于num
        if len(element) > num:
            new_group.append(element)

    if len(new_group) > 0:
        # 首先要保证有值
        # 分析区间
        for index_in_real, single_period in enumerate(new_group):
            tmp_acc_period = []
            all_points = points.filter(points.id.isin(single_period)).collect()  # 初始化一个点列表
            print("第{0}个{4}区间包含{1}个数据点，这个区间开始的时间为{2}的{3}".format(index_in_real + 1, len(single_period),
                                                                 all_points[single_period[0] - 1].Date,
                                                                 all_points[single_period[0] - 1].Time,
                                                                 "加速" if up else "减速"))
            # 在停止点中循环
            for single_single_period, id_in_single_points in enumerate(single_period):
                tmp_acc_period.append(all_points[single_period[single_single_period] - 1])
                print("--->第{0}个{4}区间的第{1}个数据点的速度为{2}，加速度为{3}".format(index_in_real + 1, single_single_period + 1,
                                                                      all_points[single_period[
                                                                                     single_single_period] - 1].speed,
                                                                      all_points[single_period[
                                                                                     single_single_period] - 1].acceleration,
                                                                      "加速" if up else "减速"))
            periods.append(tmp_acc_period)
            print(
                "第{0}个{3}区间结束的时间为{1}的{2}".format(index_in_real + 1,
                                                 all_points[single_period[len(single_period) - 1] - 1].Date,
                                                 all_points[single_period[len(single_period) - 1] - 1].Time,
                                                 "加速" if up else "减速"))
        print("===分隔线===")
        for index_in_save, single_period in enumerate(periods):
            tmpdf = pd.DataFrame(
                columns=['Latitude', 'Longitude', 'Altitude', 'Day_from1899', 'Date', 'Time', 'speed', 'acceleration',
                         'id', 'Stop', 'Speed_up'], data=single_period)

            tmpdf.to_csv('points/period/' + ("加速" if up else "减速") + '区间' + str(index_in_save + 1) + '.csv',
                         index=False)






# 之后的函数是简单的输出和保存处理
if __name__ == "__main__":
    # 读取数据，计算速度
    df = calPltSpeed("170\\Trajectory\\20080428112704.plt")
    print(df.show(5))

    # 分析停止点
    res = calc_stop_point(df)
    print(res.show(5))
    sdf = res.filter(res['Stop'] == 1)
    # 把停止点表转为list
    stop_points = sdf.collect()
    # 把静止点的序号提取成一个数组
    sp_index = []
    for single_s_point in stop_points:
        sp_index.append(single_s_point.id)

    sp_array = np.array(sp_index)
    sp_group = np.split(sp_array, np.where(np.diff(sp_array) != 1)[0] + 1)
    print(sp_group)

    # 分析加速区间
    analyse_speed(stop_points, 10, True)
    # 分析减速区间
    analyse_speed(stop_points, 10, False)
