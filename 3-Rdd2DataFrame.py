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
# 设置端口号
conf.set("spark.port.maxRetries", "128")
conf.set("spark.ui.port", "12345")
sc = SparkContext(conf=conf)  # 创建spark对象
line = sc.textFile("employee.txt").map(lambda x: x.split(",")).map(
    lambda y: Row(id=int(y[0]), name=y[1], age=int(y[2])))  # 读入文件
# 创建一个sql上下文
sql_context = SQLContext(sc)
schema_employee = sql_context.createDataFrame(line)
# 创建一个临时的表
schema_employee.createOrReplaceTempView('employee')
df_employee = sql_context.sql('select * from employee')
df_employee.show()  # 打印出所有数据
# 按规定格式打印所有结果
df_employee_alter = df_employee.collect()
for people in df_employee_alter:
    print("id:{0},name:{1},age:{2}".format(people.id, people.name, people.age))
