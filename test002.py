# coding=utf-8
from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
import os

# 注意这里右键在terminal里跑代码，并且需要在头部表明coding为utf-8

# 指定一下Java环境防止Exception: Java gateway process exited before sending its port number。
os.environ['JAVA_HOME'] = r"C://Java//jdk1.8.0_333"

# 初始化
spark = SparkSession.builder.master("[local[*]]").appName("FirstAPP").getOrCreate()

# 获取0-9的数据
data = spark.createDataFrame(map(lambda x: (x,), range(10)), ["id"])
# data = spark.range(0,10).select(col("id").cast("double"))

# 求和
data.agg({'id': 'sum'}).show()

# 关闭
spark.stop()
