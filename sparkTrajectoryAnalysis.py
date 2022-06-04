import findspark
findspark.init()

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql import Window
from itertools import islice

# 初始化SparkConf
conf = SparkConf().setMaster("local").setAppName("spark-Trajectory-Analysis")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
# 建立窗口对象，因源数据所有条目均是同一车，不需要进行分类处理，故partitionBy设为空。
my_window = Window.partitionBy().orderBy("date")


# 通过经纬度与时间差计算速度的函数
def speedCalc(lng1, lat1, lng2, lat2, timelag):
    pi = 3.1415926
    # 角度转弧度
    lng1 = lng1 * pi / 180
    lat1 = lat1 * pi / 180
    lng2 = lng2 * pi / 180
    lat2 = lat2 * pi / 180
    dlon = F.abs(lng2 - lng1)
    dlat = F.abs(lat2 - lat1)
    # 计算距离，这里的**代表平方
    angle = F.sin(dlat / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2
    distance = 2 * F.asin(F.sqrt(angle)) * 6371393  # 地球平均半径，约6371km
    # distance = F.round(distance / 1000, 5)
    speed = distance / timelag  # 距离除以时间差得到速度，这里速度未转换就是m/s
    return speed


# 计算速度值函数：车辆速率计算
def calSpeed(df):
    # 通过上面的函数来计算速度，这里除去起点终点，直接视为速度为0
    df = df.withColumn("speed", F.when(((F.col("id") > 0) & (F.col("id") < df.count() - 1)),
        speedCalc(F.lag("latitude", 1).over(my_window), F.lag("longtitude", 1).over(my_window), 
            df.latitude, df.longtitude, df.timelag)).otherwise(0))  # 计算速度
    return df


# 计算停留点函数：车辆停留点分析
def calStopPoints(df, limit = 0.4):
    # 速度阈值也可以用比率计算得到，这里则直接取得默认速度阈值为0.4m/s
    # 停留点分析：若小于阈值，则将上一个点作为停留开始点，继续遍历，只要轨迹点的速率值仍小于阈值，则将该点作为停留点。
    # 停留点标记，1为停留点，0为非停留点；判定依据：速度小于阈值，或之后的那个点速度也小于阈值
    df = df.withColumn("stop", F.when((F.col("speed") < limit) | (F.lead("speed", 1).over(
        my_window) < limit), 1).otherwise(0))
    return df


# 计算加减速函数：车辆加减速分析
def calAcceleration(df):
    # 计算加速度，速度差除时间差
    df = df.withColumn("acceleration", F.when((F.col("id") > 0),
        (df.speed - F.lag("speed", 1).over(my_window)) / df.timelag).otherwise(0))
    df = df.withColumn("direct", F.when(df.acceleration > 0, 1).otherwise(-1))  # 加速度标记，1为加速度为正

    # 计算加减速区间，加速度符号是否相等，没有变化就是1，否则是0
    df = df.withColumn("isChange",(F.col("direct") != F.lag("direct").over(my_window)).cast("int"))
    df = df.fillna(0, subset=["isChange"])  # 填充缺失值
    # 判断每一行与上下行的关系从而确定是否可以作为区间
    df = df.withColumn("inRange", F.when((F.col("id") < df.count() - 1),
        (~((F.lead("isChange", 1).over(my_window)==1) & (F.col("isChange")==1)))).otherwise(False))
    
    # 确定加速和减速区间
    df = df.withColumn("speedUp", (F.col("acceleration") > 0) & (F.col("inRange") == True))
    df = df.withColumn("speedDown", (F.col("acceleration") < 0) & (F.col("inRange") == True))
    return df
    

# 主程序
if __name__ == "__main__":
    # 读取文件的路径
    data_path = "170\\Trajectory\\20080428112704.plt"  # 为简化起见，这里选择170号数据的第一个文件作为代表
    output_csv = "result.csv"  # 输出文件的路径，这里输出为csv格式直接是文件夹

    # 文件预处理操作，这里进行一些基础的列计算
    line = sc.textFile(data_path)  # 读取文件
    # 跳过前6行说明信息，读取经度、纬度、时间信息
    data = line.map(lambda line: line.split(",")).mapPartitionsWithIndex(
        lambda i, test_iter: islice(test_iter, 6, None) if i == 0 else test_iter).map(
            lambda x: {"latitude":float(x[0]), "longtitude":float(x[1]), "date":f"{x[5]} {x[6]}"}).map(
                lambda p: Row(**(p))).toDF()

    data = data.withColumn("id", F.monotonically_increasing_id())  # 添加id列
    # 这里用unix_timestamp函数可计算时刻式组织的前后时间差
    data = data.withColumn("timelag", F.unix_timestamp(data.date) - F.unix_timestamp((F.lag("date", 1).over(my_window))))
    data = data.fillna(0, subset=["timelag"])  # 第一个点没有值，用0填充

    # 进行分析
    data = calSpeed(data)  # 计算速度
    data = calStopPoints(data)  # 计算停留点
    data = calAcceleration(data)  # 计算加减速区间

    # 找出全部停留点并输出至控制台
    stop_points = data.filter(data.stop == 1)
    for each in stop_points.collect():
        print("第{0}个轨迹点,为车辆停留点,时刻为{1},速度为{2}m/s".format(
            each.id, each.date, round(each.speed, 5)))  # 四舍五入速度保留5位小数

    # 将数据写入csv单文件，这里如果直接用spark的csv()方法得到的csv结果是个多文件文件夹，故转成pandas再转成csv输出
    pd_data = data.toPandas()  # 转换为pandas数据
    pd_data.to_csv(output_csv, mode='a', header='true', index=False, encoding='utf-8-sig')

    
    # 把各个加减速区间合并输出，只要ischange改变了一次，就会增多一个区间，直接作为序号即可
    df = data.filter(data.inRange == 1)  # 过滤掉单个点的情况，只保留区间
    df = df.withColumn("rangeIndex", F.when((F.col("direct") == F.lead("direct", 1).over(my_window)) | \
            (F.col("direct") == F.lag("direct", 1).over(my_window)), F.sum(F.col("isChange")).over(
                my_window.rangeBetween(Window.unboundedPreceding, 0))).otherwise(0))  # 计算区间序号
    
    df = df.filter(df.rangeIndex != 0)  # 只保留有加减速区间的数据
    # 在控制台输出区间
    for each in df.collect():
        if each.speedUp == 1:
            print("第{0}个轨迹点,处于第{1}个区间,该区间为{2}区间,时刻为{3},速度为{4}m/s".format(
                each.id, each.rangeIndex, "加速", each.date, round(each.speed, 5)))
        elif each.speedDown == 1:
            print("第{0}个轨迹点,处于第{1}个区间,该区间为{2}区间,时刻为{3},速度为{4}m/s".format(
                each.id, each.rangeIndex, "减速", each.date, round(each.speed, 5)))
