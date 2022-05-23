#coding=utf-8

#/usr/local/spark/mycode/python/WordCount.py
# 统计含有特有字符的文件行数
import findspark
findspark.init()

if __name__ == '__main__':
  from pyspark import SparkConf, SparkContext
  conf = SparkConf().setMaster("local").setAppName("My App")
  sc = SparkContext(conf = conf)
  logFile = "D://CodeWorkSpace//py2_spark//testData.txt"
  logData = sc.textFile(logFile, 2).cache()
  numAs = logData.filter(lambda line: 'a' in line).count()
  numBs = logData.filter(lambda line: 'b' in line).count()
  print('Lines with a: %s, Lines with b: %s' % (numAs, numBs))
