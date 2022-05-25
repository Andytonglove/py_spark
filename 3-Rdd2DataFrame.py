# -*- coding: utf-8 -*-
import sys
import imp
imp.reload(sys)
sys.setdefaultencoding("utf-8")

import findspark
findspark.init()

'''
2. 编程实现将RDD转换为DataFrame
实现从RDD转换得到DataFrame
并按“id:1,name:Ella,age:36”的格式打印出DataFrame的所有数据。
'''

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row

conf = SparkConf().setMaster("local").setAppName("RDD2DataFrame")

conf.set("spark.port.maxRetries", "100")
conf.set("spark.ui.port", "4040")  # 设置spark-web的端口

sc = SparkContext(conf=conf)  # 创建spark对象
line = sc.textFile("employee.txt").map(lambda x: x.split(",")).map(
    lambda y: Row(id=int(y[0]), name=y[1], age=int(y[2])))  # 读入文件

sql_context = SQLContext(sc)  # 创建sql对象以进行执行
schema_employee = sql_context.createDataFrame(line)

schema_employee.createOrReplaceTempView('employee')  # 创建临时表
df_employee = sql_context.sql('select * from employee')
df_employee.show()  # 打印出所有数据
# 按规定格式打印所有结果
sort_df_employee = df_employee.collect()
for employees in sort_df_employee:
    print("id:{0},name:{1},age:{2}".format(employees.id, employees.name, employees.age))
