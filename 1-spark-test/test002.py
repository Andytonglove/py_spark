# coding=utf-8
import findspark
findspark.init()

from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# 初始化
spark = SparkSession.builder.master("local[*]").appName("FirstAPP").getOrCreate()

# 获取0-9的数据
data = spark.createDataFrame(map(lambda x: (x,), range(10)), ["id"])
# data = spark.range(0,10).select(col("id").cast("double"))

# 求和
data.agg({'id': 'sum'}).show()

# 关闭
spark.stop()
