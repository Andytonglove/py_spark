# -*- coding: utf-8 -*-
import sys
import imp
imp.reload(sys)
sys.setdefaultencoding("utf-8")

import findspark
findspark.init()

import math
# 导入pyspark模块
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType

'''
基于Spark Structured Streaming,编写车辆轨迹处理程序,实时计算车辆进行速度。
假设出发是车速为0,每收到一条对应车辆的坐标信息,就根据收到的坐标点和上一次的坐标点计算之间距离,
然后距离除以时间差,作为当前车速。
'''

def dist(lat1, lon1, lat2, lon2):
    # 计算两点间距离 wgs84 单位:m
    # 传入参数为第一点和第二点的纬度和经度列
    # TODO
    dist = []
    for i in lat1.collect():
        print(lat1.head(i))
        lat1 = float(lat1.head(i))
        lat2 = float(lat2.head(i))
        lon1 = float(lon1.head(i))
        lon2 = float(lon2.head(i))
        R = 6371
        dLat = (lat2 - lat1) * math.pi / 180.0
        dLon = (lon2 - lon1) * math.pi / 180.0

        a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(lat1 * math.pi / 180.0) * math.cos(
            lat2 * math.pi / 180.0) * math.sin(dLon / 2) * math.sin(dLon / 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        dist[i] = R * c
    return dist * 1000


if __name__ == "__main__":
    # 创建一个SparkSession对象
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2") \
        .appName("pyspark_structured_streaming_kafka") \
        .getOrCreate() 

    spark.sparkContext.setLogLevel('WARN')
    # 创建输入数据源，定义配套dataFrame；从kafka的traffic主题中接收消息。
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "traffic") \
            .load()
    words = df.selectExpr("CAST(value AS STRING)")
    schema = StructType() \
            .add("lat1", StringType()) \
            .add("lon1", StringType()) \
            .add("lat2", StringType()) \
            .add("lon2", StringType()) \
            .add("time1", StringType()) \
            .add("time2", StringType()) \
            .add("dist", StringType())
    # 定义流计算过程
    res = words.select(from_json("value", schema).alias("data")).select("data.*")
    # 获取每一行的值
    res = res.selectExpr("CAST(lat1 AS DOUBLE)", "CAST(lon1 AS DOUBLE)", "CAST(lat2 AS DOUBLE)", 
        "CAST(lon2 AS DOUBLE)", "CAST(time1 AS STRING)", "CAST(time2 AS STRING)", "CAST(dist AS DOUBLE)")

    # 计算两点之间的距离，注意这里返回的是整个列即'Column' object，需要进行处理
    res = res.withColumn("distance", dist(res.lat1, res.lon1, res.lat2, res.lon2).toDF("distance"))

    # 计算时间差
    res = res.withColumn("time_diff", 86400*(res.time1 - res.time2).cast("double"))
    # 计算速度
    res = res.withColumn("speed", (res.dist / res.time_diff).cast("double"))
    # 将结果打印到控制台中
    query = res.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    query.awaitTermination()
    spark.stop()
