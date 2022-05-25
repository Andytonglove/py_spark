# -*- coding: utf-8 -*-
import sys
import imp
imp.reload(sys)
sys.setdefaultencoding("utf-8")

import findspark
findspark.init()

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
# 创建testspark数据库testspark，并使用
cursor.execute('CREATE DATABASE IF NOT EXISTS testspark;')
cursor.execute('USE testspark;')
# 创建数据表employee
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
conf = SparkConf().setMaster("local").setAppName("sparkMysql")
conf.set("spark.port.maxRetries", "128")
conf.set("spark.ui.port", "12345")
sc = SparkContext(conf=conf)  # 创建spark对象
spark = SQLContext(sc)
# mysql 配置，连接用户和jdbc
prop = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.jdbc.Driver"
}
# 连接mysql数据库，这里需要先添加jdbc的依赖包到spark/jars中，这里加入useSSL去除安全提示
url = 'jdbc:mysql://localhost:3306/testspark?serverTimezone=UTC&useSSL=false'

# 读取表并插入数据
employeeRDD = sc.parallelize(["Mary F 26", "Tom M 23"]).map(lambda x: x.split(" ")).map(
    lambda p: Row(name=p[0].strip(), gender=p[1].strip(), Age=int(p[2].strip())))
schema_employee = spark.createDataFrame(employeeRDD)
# 创建一个临时表
schema_employee.createOrReplaceTempView('employee')
employeeDF = spark.sql('select * from employee')
employeeDF.show()
employeeDF.write.jdbc(url=url, table='employee', mode='append', properties=prop)

# 求Age的最大值
age_max = spark.read.format("jdbc").options(
    url='jdbc:mysql://localhost:3306/testspark?serverTimezone=UTC&user=root&password=root&useSSL=false',
    dbtable="(SELECT max(Age) FROM employee) tmp",
    driver='com.mysql.jdbc.Driver').load()
age_max.show()

# 求Age的总和
age_sum = spark.read.format("jdbc").options(
    url='jdbc:mysql://localhost:3306/testspark?serverTimezone=UTC&user=root&password=root&useSSL=false',
    dbtable="(SELECT sum(Age) FROM employee) tmp",
    driver='com.mysql.jdbc.Driver').load()
age_sum.show()
sc.stop()