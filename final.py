'''
需求：
1. 车辆速率计算
从原始轨迹数据的经纬度坐标计算每个轨迹点与前一个点的距离，再除以两个轨迹点的时间差，得到的值作为该点的速率值保存。
每个轨迹点的即时速率通过计算该点与后一个点速率值的平均值每个轨迹点的即时速率通过计算该点与后一个点速率值的平均值得到，
根据实际意义，规定起点和终点的即时速率为零。得到，根据实际意义，规定起点和终点的即时速率为零。

2. 车辆停留点停留点分析
需将停留点和移动点区分开，停留点具有开始时刻和结束时刻两个时间属性。
停留点识别的算法描述如下：
1) 遍历轨迹点，检查速率值，若小于阈值，则将上一个点作为停留开始点，记录其序号。
2) 继续遍历轨迹点，只要轨迹点的速率值仍小于阈值，则累加它的距离值。

3.车辆加减速分析
加速和减速都是持续性的过程，而突发的速率变化则可能是数据采集或预处理阶段的误差引起的正常波动，
因此加减速检测方法可以采用寻找至少连续N（N≥2）个点速率单调变化的片段。
'''

import math


def dist_ll(lat1, lon1, lat2, lon2):
    """
    计算两点间距离（wgs84），单位：m
    :param lat1: 第一点纬度
    :param lon1: 第一点经度
    :param lat2: 第二点纬度
    :param lon2: 第二点经度
    :return:
    """
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

def read_car_plt(path):
    """
    把plt文件读成spark dataframe
    :param path: 文件路径
    :return:
    """
    pandas_df = pd.read_csv(path, header=None, skiprows=6, usecols=[0, 1, 3, 4, 5, 6],
                            names=['Latitude', 'Longitude', 'Altitude', 'Day_from1899', 'Date',
                                   'Time'])
    # 实现要求1
    # 计算速度，开始和最后都是0
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
    return sql_context.createDataFrame(pandas_df)

def calc_stop_point(df):
    """
    输入一个dataframe，输出一个含有停留点信息的dataframe
    :return:
    """
    return df.withColumn('Stop', df.speed < 0.4)

sdf = res.filter(res['Stop'] == 1)
# 把停止点表转为list
stop_points = sdf.collect()
# 把静止点的序号提取成一个数组
sp_index = []
for single_s_point in stop_points:
    sp_index.append(single_s_point.id)

sp_array = np.array(sp_index)
sp_group = np.split(sp_array, np.where(np.diff(sp_array) != 1)[0] + 1)

# 单调区间分析函数
def analyse_speed(points, num, up):
    """
    分析单调区间，并保存成文件
    :param points:输入spark DF的点列表
    :param num:输入最少数目
    :param up:布尔值，true加速false减速
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

    # 创建一个空列表
    new_group = []

    # 遍历 arr 中的每个元素
    for element in p_group:
        # 如果元素长度大于num
        if len(element) > num:
            new_group.append(element)