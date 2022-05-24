# -*- coding: utf-8 -*-
import sys
import imp
imp.reload(sys)
sys.setdefaultencoding("utf-8")

import findspark
findspark.init()

'''
1. Spark SQL 基本操作
为employee.json创建DataFrame
并通过Python语句完成如下10个SQL操作
'''

from pyspark import SparkConf
from pyspark.sql import SparkSession

# 创建SparkSession对象
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
# 创建一个DataFrame对象
df_origin = spark.read.json("employee.json")

print("1.查询所有数据:")
df_origin.select("*").show()  # 查询所有数据

print("2.查询所有数据,并去除重复的数据:")
# 逻辑上，年龄，姓名有可能重复，id不能重复
df_unique = df_origin.drop_duplicates(subset=['id'])
df_unique.select("*").show()  # 查询所有数据，并去除重复的数据

print("3.查询所有数据,打印时去除id字段:")
df_1 = df_origin.select('*')
df_1.drop(df_1.id).show()  # 先选取所有数据，利用drop()方法去除id字段
df_origin.select('name', 'age').show()  # 查询所有数据，打印时去除id字段

print("4.筛选出age>30的记录:")
df_origin.filter(df_origin['age'] > 30).show()  # 筛选出age>30的记录

print("5.将数据按age分组:")
df_origin.groupby('age').count().show()  # 将数据按age分组

print("6.将数据按name升序排列")
df_origin.sort(df_origin['name'].asc()).show()  # 将数据按name升序排列

print("7.取出前3行数据")
data = df_origin.head(3)  # 取出前3行数据
for i in data:
    print(i)  # 遍历打印前3行数据
print("\n")

print("8.查询所有记录的name列,并为其取别名为username")
name = df_origin.select('name')  # 查询所有记录的name列
name.withColumnRenamed('name', 'username').show()  # 将name列取别名为username

print("9.查询年龄age的平均值")
df_origin.agg({'age': 'mean'}).show()  # 查询年龄age的平均值

print("10.查询年龄age的最小值。")
df_origin.agg({'age': 'min'}).show()  # 查询年龄age的最小值。
