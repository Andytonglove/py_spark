import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
import pyspark.sql.functions as fun
from pyspark.sql import SQLContext,Window, Row
from itertools import islice

# spark 入口
conf = SparkConf().setMaster("local").setAppName("spark-Trajectory-Analysis")
sc = SparkContext(conf = conf)  
sqlContext = SQLContext(sc)

# 建立窗口对象，因源数据所有条目均是同一车，不需要进行分类处理，故partitionBy设为空。
my_window = Window.partitionBy().orderBy("date")

# 计算速度的函数，输入为前后两个点的经纬度和时间差
def cal_speed(lat1, lon1, lat2, lon2, dtime):
    pi = 3.1415926
    lat1 = lat1 / 180*pi
    lon1 = lon1 / 180*pi
    lat2 = lat2 / 180*pi    
    lon2 = lon2 / 180*pi

    earth_r = 6371393

    dlat = fun.abs(lat2 - lat1)
    dlon = fun.abs(lon2 - lon1)
    
    angle = fun.sin(dlat / 2) * fun.sin(dlat / 2) + fun.cos(lat1) * fun.cos(
        lat2) * fun.sin(dlon / 2) * fun.sin(dlon / 2)
    c = 2 * fun.atan2(fun.sqrt(angle), fun.sqrt(1 - angle))

    dist = earth_r * c

    speed = dist / dtime
    return speed


#通过经纬度与时间差计算速度函数
def speedCalc(lng1, lat1, lng2, lat2, timeback, timenow):
    pi = 3.1415926
    #角度转弧度
    lng1 = lng1 * pi / 180
    lat1 = lat1 * pi / 180
    lng2 = lng2 * pi / 180
    lat2 = lat2 * pi / 180
    dlon = fun.abs(lng2 - lng1)
    dlat = fun.abs(lat2 - lat1)
    # 计算距离，这里的**代表乘方
    angle = fun.sin(dlat / 2) ** 2 + fun.cos(lat1) * fun.cos(lat2) * fun.sin(dlon / 2) ** 2
    distance = 2 * fun.asin(fun.sqrt(angle)) * 6371393  # 地球平均半径，约6371km
    distance = fun.round(distance / 1000, 5)  # 五位小数，单位千米
    speed = distance / ((timenow - timeback) * 24)  # 距离除以时间差（小时）得到时速
    return speed


def calc_stop_point(df):
    # 输入一个dataframe，输出一个含有停留点信息的dataframe
    return df.withColumn('Stop', df.speed < 0.4)


def cal_acceleration(speed1, speed2, dtime):
    # 计算加速度函数，输入为前后两个点的速度和时间差
    return (speed2 - speed1) / dtime


# 初始化df, 包括读取文件, 列处理等步骤
def df_init(path):
    # 读取文件
    line = sc.textFile(path)

    # 读取了经度、纬度、日期和时间，并将时间和日期合并为一列
    dataSet = line.map(lambda line: line.split(",")).mapPartitionsWithIndex(
    lambda i, test_iter: islice(test_iter, 6, None) if i == 0 else test_iter).\
    map(lambda x: {"latitude":float(x[0]), "longtitude":float(x[1]),\
        "date":f"{x[5]} {x[6]}"}).\
    map(lambda p: Row(**(p))).toDF()

    # 添加时间戳列
    dataSet = dataSet.\
    withColumn("unix_time", fun.unix_timestamp("date", "yyyy-MM-dd HH:mm:ss")).\
    withColumn("id", fun.monotonically_increasing_id())
    # 计算前后点的时间差，并添加至df中
    dataSet = dataSet.withColumn("dtime", dataSet.unix_time - fun.lag("unix_time", 1).over(my_window))
    # 将时间差为空的值赋值0
    dataSet = dataSet.fillna(0, subset=["dtime"])

    return dataSet


# 确定停留点速度阈值, rate是速度排名(从低到高)百分比
def determine_threshold(df, rate, default_thres=0.4):
    # 若rate不为0, 根据rate确定速度阈值
    if rate != 0.0:
        df_len = df.count()
        speed_num = int(df_len * (1-rate))
        df.select(max(['speed']))

        df = df.select("speed",fun.rank().over(my_window).alias("rank"))
        rank = df.select("speed").collect()
        rank_list = [row[0] for row in rank]
        threshold = rank_list[speed_num]

        return threshold
    # 若rate等于0, 返回给定的速度阈值
    else:
        return default_thres


# 将df中的列表转为string, 否则无法直接写入csv
def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'

def merge_pts(df):
    return True



def calSpeed(df):
    # 计算速度列(将起点和终点速度置为0)
    row_number = df.count()
    df = df.withColumn("speed",fun.when(((fun.col("id")<row_number-1)&(fun.col("id")>0)),
        cal_speed(fun.lag("latitude", 1).over(my_window),
            fun.lag("longtitude", 1).over(my_window),
            df.latitude, df.longtitude, df.dtime)).\
            otherwise(0)) 
    return df


def calStopPoints(df):
    # 停留点分析
    speed_threshold = determine_threshold(df, 0.01)
    df = df.\
        withColumn("stop_flag",
        fun.when((fun.lead("speed", 1).over(my_window) < speed_threshold)|\
        (fun.col("speed") < speed_threshold), 1 ).\
        otherwise(0))

    return df


def calAcceleration(df):
    # 加速度计算
    df = df.\
        withColumn("acceleration",
        fun.when((fun.col("id")>0), 
        cal_acceleration(fun.lag("speed", 1).over(my_window),df.speed,df.dtime)).\
        otherwise(0))

    df = df.drop("dtime").drop("unix_time")
    
    df = df.\
        withColumn("flag", 
        fun.when(df.acceleration > 0, 1).\
        otherwise(-1))

    df = df.withColumn(
            "flagChange",
            (fun.col("flag") != fun.lag("flag").over(my_window)).cast("int")
        )\
        .fillna(
            0,
            subset=["flagChange"]
        )

    df = df.withColumn("indicator",\
        fun.when((fun.col("id")<df.count()-1),
        (~((fun.lead("flagChange",1).over(my_window)==1)&(fun.col("flagchange")==1))))\
        .otherwise(False))

    # df = df.filter(df["indicator"])

    return df


def clustering(df):
    df = df.withColumn(
            "interval_number", fun.when((fun.col("flag")==fun.lead("flag",1).over(my_window))|\
            (fun.col("flag")==fun.lag("flag",1).over(my_window)),fun.sum(fun.col("flagChange")).\
            over(my_window.rangeBetween(Window.unboundedPreceding, 0))).otherwise(0))

    df = df.filter(df["interval_number"]!=0)

    return df
    

# 读取文件的路径
file_path = "170\\Trajectory\\20080428112704.plt"  # 为简化起见，这里选择170号数据的第一个文件
csv_path = "result\\result.csv"
# 进行文件读取处理操作
dataSet = df_init(file_path)
# 计算速度
dataSet = calSpeed(dataSet)
# 计算停留点
dataSet = calStopPoints(dataSet)
# 计算加速度
dataSet = calAcceleration(dataSet)
# 计算聚类，写入文件
dataSet.write.mode("append").option("header","true").option("encoding","utf-8").csv(csv_path)
