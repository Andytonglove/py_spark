from base64 import encode
import encodings
from msilib import datasizemask
import findspark

findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,Window, Row
import pyspark.sql.functions as func
from itertools import islice
from pyspark.sql.types import StringType
import folium
import os


# spark 入口
conf = SparkConf().setMaster("local").setAppName("Final_Project").\
    set("spark.executor.heartbeatInterval", "200000").\
    set("spark.network.timeout", "300000")

sc = SparkContext(conf=conf)  
sqlContext = SQLContext(sc)

# 建立窗口对象，因源数据所有条目均是同一车，不需要进行分类处理，故partitionBy设为空。
my_window = Window.partitionBy().orderBy("date")


def cal_speed(lat1, lon1, lat2, lon2, dtime):
    pi = 3.1415926
    lat1 = lat1 / 180*pi
    lon1 = lon1 / 180*pi
    lat2 = lat2 / 180*pi    
    lon2 = lon2 / 180*pi

    earth_r = 6371393

    dlat = func.abs(lat2 - lat1)
    dlon = func.abs(lon2 - lon1)

    # angle = 2 * func.asin(func.sqrt(func.sin(dlat/2)*func.sin(dlat/2)+
    #     func.cos(lat1)*func.cos(lon1)*func.sin(dlon/2)*func.sin(dlon/2)))
    
    a = func.sin(dlat / 2) * func.sin(dlat / 2) + func.cos(lat1) * func.cos(
        lat2) * func.sin(dlon / 2) * func.sin(dlon / 2)
    c = 2 * func.atan2(func.sqrt(a), func.sqrt(1 - a))
    # p_dist = angle * earth_r
    dist = earth_r * c

    speed = dist / dtime
    return speed


def calc_stop_point(df):
    """
    输入一个dataframe，输出一个含有停留点信息的dataframe
    :return:
    """
    return df.withColumn('Stop', df.speed < 0.4)


def cal_acceleration(speed1, speed2, dtime):
    return (speed2-speed1)/dtime


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
    withColumn("unix_time", func.unix_timestamp("date", "yyyy-MM-dd HH:mm:ss")).\
    withColumn("id", func.monotonically_increasing_id())
    # 计算前后点的时间差，并添加至df中
    dataSet = dataSet.withColumn("dtime", dataSet.unix_time - func.lag("unix_time", 1).over(my_window))
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

        df = df.select("speed",func.rank().over(my_window).alias("rank"))
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



def question1_speed(df):
    # 计算速度列(将起点和终点速度置为0)
    row_number = df.count()
    df = df.\
        withColumn("speed",func.when(((func.col("id")<row_number-1)&(func.col("id")>0)),
        cal_speed(func.lag("latitude", 1).over(my_window),
            func.lag("longtitude", 1).over(my_window),
            df.latitude, df.longtitude, df.dtime)).\
            otherwise(0)) 
    return df


def question2_stop_pts(df):
    # 停留点分析
    speed_threshold = determine_threshold(df, 0.01)
    df = df.\
        withColumn("stop_flag",
        func.when((func.lead("speed", 1).over(my_window) < speed_threshold)|\
        (func.col("speed") < speed_threshold), 1 ).\
        otherwise(0))

    return df


def question3_acceleration(df):
    # 加速度计算
    df = df.\
        withColumn("acceleration",
        func.when((func.col("id")>0), 
        cal_acceleration(func.lag("speed", 1).over(my_window),df.speed,df.dtime)).\
        otherwise(0))

    df = df.drop("dtime").drop("unix_time")
    
    df = df.\
        withColumn("flag", 
        func.when(df.acceleration > 0, 1).\
        otherwise(-1))

    df = df.withColumn(
            "flagChange",
            (func.col("flag") != func.lag("flag").over(my_window)).cast("int")
        )\
        .fillna(
            0,
            subset=["flagChange"]
        )

    df = df.withColumn("indicator",\
        func.when((func.col("id")<df.count()-1),
        (~((func.lead("flagChange",1).over(my_window)==1)&(func.col("flagchange")==1))))\
        .otherwise(False))

    # df = df.filter(df["indicator"])

    return df


def clustering(df):
    df = df.withColumn(
            "interval_number", func.when((func.col("flag")==func.lead("flag",1).over(my_window))|\
            (func.col("flag")==func.lag("flag",1).over(my_window)),func.sum(func.col("flagChange")).\
            over(my_window.rangeBetween(Window.unboundedPreceding, 0))).otherwise(0))

    df = df.filter(df["interval_number"]!=0)

    return df
    

# 读取文件的路径
file_path = "data/20080428112704.plt"
csv_path = "D:/Python/SpatiotemporalData/data/test.csv"

dataSet = df_init(file_path)

dataSet = question1_speed(dataSet)

dataSet = question2_stop_pts(dataSet)

dataSet = question3_acceleration(dataSet)

dataSet.write.mode("append").option("header","true").option("encoding","utf-8").csv(csv_path)



