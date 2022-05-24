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
    print(i)

print("8.查询所有记录的name列,并为其取别名为username")
name = df_origin.select('name')  # 查询所有记录的name列
name.withColumnRenamed('name', 'username').show()  # 将name列取别名为username

print("9.查询年龄age的平均值")
df_origin.agg({'age': 'mean'}).show()  # 查询年龄age的平均值

print("10.查询年龄age的最小值。")
df_origin.agg({'age': 'min'}).show()  # 查询年龄age的最小值。



'''
2. 编程实现将RDD转换为DataFrame
实现从RDD转换得到DataFrame
并按“id:1,name:Ella,age:36”的格式打印出DataFrame的所有数据。
'''

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row

conf = SparkConf().setMaster("local").setAppName("rdd2df")
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
# 先用默认方式显示一下，哦，完美
df_employee.show()
# 再按照要求的方式输出，其实collect很浪费性能，但是数据量很小，就原谅一下吧
df_employee_alter = df_employee.collect()
for people in df_employee_alter:
    print("id:{0},name:{1},age:{2}".format(people.id, people.name, people.age))


'''
3. 编程实现利用DataFrame读写MySQL的数据
在MySQL数据库中新建数据库testspark,再创建表employee
配置Spark通过JDBC连接数据库MySQL
编程实现利用DataFrame插入两行新增数据到MySQL中
最后打印出age的最大值和age的总和。
'''

import pymysql

conn = pymysql.connect(host='localhost', port=3306, user="root", passwd="root")
# 获取游标
cursor = conn.cursor()
# 创建testspark数据库，并使用
cursor.execute('CREATE DATABASE IF NOT EXISTS testspark;')
cursor.execute('USE testspark;')
sql = "CREATE TABLE IF NOT EXISTS employee (id int(3) NOT NULL AUTO_INCREMENT,name varchar(255) NOT NULL,gender char(1) NOT NULL,Age int(3) NOT NULL,PRIMARY KEY (id))"
cursor.execute(sql)
cursor.execute("INSERT INTO employee (name,gender,Age) VALUES ('Alice','F',22)")
cursor.execute("INSERT INTO employee (name,gender,Age) VALUES ('John','M',25)")
# commit更改
conn.commit()
cursor.close()  # 先关闭游标
conn.close()  # 再关闭数据库连接

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

# spark 初始化
conf = SparkConf().setMaster("local").setAppName("sparksql")
conf.set("spark.port.maxRetries", "128")
conf.set("spark.ui.port", "12345")
sc = SparkContext(conf=conf)  # 创建spark对象
spark = SQLContext(sc)
# mysql 配置
prop = {'user': 'root',
        'password': 'root',
        'driver': 'com.mysql.cj.jdbc.Driver'}
# database
url = 'jdbc:mysql://localhost:3306/testspark?serverTimezone=UTC'

# 读取表
employeeRDD = sc.parallelize(["Mary F 26", "Tom M 23"]).map(lambda x: x.split(" ")).map(
    lambda p: Row(name=p[0].strip(), gender=p[1].strip(), Age=int(p[2].strip())))
schema_employee = spark.createDataFrame(employeeRDD)
# 创建一个临时表
schema_employee.createOrReplaceTempView('employee')
employeeDF = spark.sql('select * from employee')
employeeDF.show()
employeeDF.write.jdbc(url=url, table='employee', mode='append',
                      properties=prop)

age_sum = spark.read.format("jdbc").options(
    url='jdbc:mysql://localhost:3306/testspark?serverTimezone=UTC&user=root&password=root',
    dbtable="(SELECT sum(Age) FROM employee) tmp",
    driver='com.mysql.cj.jdbc.Driver').load()
age_sum.show()
sc.stop()