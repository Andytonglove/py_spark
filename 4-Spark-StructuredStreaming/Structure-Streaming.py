#coding:utf-8
import findspark
findspark.init()

# 导入pyspark模块
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType

if __name__ == '__main__':
    # 创建一个SparkSession对象
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2") \
        .appName("pyspark_structured_streaming_kafka") \
        .getOrCreate() 

    spark.sparkContext.setLogLevel('WARN')
    # 创建输入数据源，定义配套dataFrame；从kafka的test主题中接收消息。
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "test") \
            .load()
    words = df.selectExpr("CAST(value AS STRING)")
    schema = StructType() \
            .add("name", StringType()) \
            .add("age", StringType()) \
            .add("sex", StringType())
    # 定义流计算过程
    res = words.select(from_json("value", schema).alias("data")).select("data.*")
    # 启动流计算并输出结果
    # 默认是微批处理模式，可以设置连续处理模式，需要和检查点配合。
    query = res.writeStream \
            .format("console") \
            .outputMode("append") \
            .start()

    query.awaitTermination()

