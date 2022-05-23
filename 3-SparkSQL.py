'''
1. Spark SQL 基本操作
将下列JSON 格式数据复制到Linux 系统中，并保存命名为employee.json。

{ "id":1 , "name":" Ella" , "age":36 }
{ "id":2, "name":"Bob","age":29 }
{ "id":3 , "name":"Jack","age":29 }
{ "id":4 , "name":"Jim","age":28 }
{ "id":4 , "name":"Jim","age":28 }
{ "id":5 , "name":"Damon" }
{ "id":5 , "name":"Damon" }

为employee.json 创建DataFrame，并写出Python 语句完成下列操作：
(1) 查询所有数据；
(2) 查询所有数据，并去除重复的数据；
(3) 查询所有数据，打印时去除id 字段；
(4) 筛选出age>30 的记录；
(5) 将数据按age 分组；
(6) 将数据按name 升序排列；
(7) 取出前3 行数据；
(8) 查询所有记录的name 列，并为其取别名为username；
(9) 查询年龄age 的平均值；
(10) 查询年龄age 的最小值。
'''

import findspark

findspark.init()
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
# 创建一个dataframe对象
df_origin = spark.read.json("employee.json")
# (1) 查询所有数据；
print("(1) 查询所有数据")
# 其实本来不用select("*")，既然强调查询，就加上也行
df_origin.select("*").show()
# (2) 查询所有数据，并去除重复的数据；
print("(2) 查询所有数据，并去除重复的数据")
# 逻辑上，年龄，姓名有可能重复，id不能重复
df_unique = df_origin.drop_duplicates(subset=['id'])
df_unique.select("*").show()
# (3) 查询所有数据，打印时去除id 字段；
print("(3) 查询所有数据，打印时去除id 字段")
# 选取所有数据
df_1 = df_origin.select('*')
# 去除id字段
df_1.drop(df_1.id).show()
print("有一种更单纯的方法")
df_origin.select('name', 'age').show()
# (4) 筛选出age>30 的记录；
print("(4) 筛选出age>30 的记录")
df_origin.filter(df_origin['age'] > 30).show()
# (5) 将数据按age 分组；
print("(5) 将数据按age 分组")
df_origin.groupby('age').count().show()
# (6) 将数据按name 升序排列；
print("(6) 将数据按name 升序排列")
df_origin.sort(df_origin['name'].asc()).show()
# (7) 取出前3行数据；
print("(7) 取出前3行数据")
var = df_origin.head(3)
for i in var:
    print(i)
# (8) 查询所有记录的name 列，并为其取别名为username；
print("(8) 查询所有记录的name 列，并为其取别名为username")
name = df_origin.select('name')
# 把取别名理解成改名，在c或者java里面，“别名”可能会被理解为浅拷贝，或者说引用，在这里我不知道该怎么理解这个词，思前想后，我将其理解为重命名
name.withColumnRenamed('name', 'username').show()
# (9) 查询年龄age 的平均值；
print("(9) 查询年龄age 的平均值")
df_origin.agg({'age': 'mean'}).show()
# (10) 查询年龄age 的最小值。
print("(10) 查询年龄age 的最小值。")
df_origin.agg({'age': 'min'}).show()


'''
2. 编程实现将RDD 转换为DataFrame
源文件内容如下（包含id,name,age）：
1,Ella,36
2,Bob,29
3,Jack,29
请将数据复制保存到Linux 系统中，命名为employee.txt，实现从RDD 转换得到
DataFrame，并按“id:1,name:Ella,age:36”的格式打印出DataFrame 的所有数据。请写出程序代
码。
'''

import findspark

findspark.init()
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
3. 编程实现利用DataFrame 读写MySQL 的数据
（1）在MySQL 数据库中新建数据库testspark，再创建表employee，包含如下表所示的两
行数据。
表1 employee 表原有数据
id name gender Age
1 Alice F 22
2 John M 25
（2）配置Spark 通过JDBC 连接数据库MySQL，编程实现利用DataFrame 插入如下表所示
的两行数据到MySQL 中，最后打印出age 的最大值和age 的总和。
表2employee 表新增数据
id name gender age
3 Mary F 26
4 Tom M 23
'''

import pymysql

conn = pymysql.connect(host='localhost', port=3306, user="root", passwd="Test123456")
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
import findspark

findspark.init()
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
        'password': 'Test123456',
        'driver': 'com.mysql.cj.jdbc.Driver'}
# database
url = 'jdbc:mysql://localhost:3306/testspark?serverTimezone=UTC'

# 读取表
employeeRDD = sc.parallelize(["Mary F 26", "Tom M 23"]).map(lambda x: x.split(" ")).map(
    lambda p: Row(name=p[0].strip(), gender=p[1].strip(), Age=int(p[2].strip())))
schema_employee = spark.createDataFrame(employeeRDD)
# 创建一个临时的表
schema_employee.createOrReplaceTempView('employee')
employeeDF = spark.sql('select * from employee')
employeeDF.show()
employeeDF.write.jdbc(url=url, table='employee', mode='append',
                      properties=prop)

age_sum = spark.read.format("jdbc").options(
    url='jdbc:mysql://localhost:3306/testspark?serverTimezone=UTC&user=root&password=Test123456',
    dbtable="(SELECT sum(Age) FROM employee) tmp",
    driver='com.mysql.cj.jdbc.Driver').load()
age_sum.show()
sc.stop()