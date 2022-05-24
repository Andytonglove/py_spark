#coding=utf-8
import sys,os

SPARK_HOME_PATH=r"D://spark-2.3.2-bin-hadoop2.7"
HADOOP_HOME_PATH=r"D://winutils//hadoop-2.7.3"
JAVA_HOME=r"C://Java//jdk1.8.0_333"

os.environ['SPARK_HOME']=SPARK_HOME_PATH
os.environ['HADOOP_HOME']=HADOOP_HOME_PATH
os.environ['JAVA_HOME']=JAVA_HOME

if not SPARK_HOME_PATH in sys.path:
    sys.path.append(SPARK_HOME_PATH)
if not HADOOP_HOME_PATH in sys.path:
    sys.path.append(HADOOP_HOME_PATH)

import findspark
findspark.init()

from pyspark.sql import SparkSession
# 初始化
spark = SparkSession.builder.getOrCreate()
df = spark.sql('''select 'spark' as hello ''')
df.show()
spark.stop()