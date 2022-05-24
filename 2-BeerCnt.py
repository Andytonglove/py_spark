# -*- coding: utf-8 -*-
import sys
import imp
imp.reload(sys)
sys.setdefaultencoding("utf-8")
# 特别的，vscode控制台出现中文乱码情况，可通过chcp 65001设置编码为utf-8解决

import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import pandas as pd
import os

# 根据给定的啤酒销售数据和去年同期销量数据，把 xlsx 文件转换为 TXT 文件，针对 11
# 月份啤酒销售数据，并通过编程进行数据处理和计算。

def xlsx2txt(input_filename, output_filename):
    # 此函数用于将xlsx文件转换为txt文件
    input_file_name = os.path.splitext(input_filename)[0]
    output_file_name = os.path.splitext(output_filename)[0]
    print("正在转换文件：" + input_file_name + ".xlsx")
    # 开始转换
    if not os.path.exists(output_file_name + '.txt'):
        # 使用pandas模块读取数据，并写入文件
        df = pd.read_excel(input_filename, sheet_name='Sheet1', header=None, skiprows=1)
        df.to_csv(output_file_name + '.txt', header=None, sep='\t', index=False)  # sep用换行符分隔
    print("转换完成！写入文件：" + output_file_name + '.txt')


def remove0sales():
    # 1）去除整月销量为 0 的数据
    # 如果过去三周平均销量为0，则月销量即为0，将其过滤去除
    lines = line1.filter(lambda x: "0" != x[3])
    for item in lines.collect():
        # 使用format格式化输出
        print("{0},{1},{2},{3},{4},{5},{6},{7}".format(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7]))


# 转换数值格式的处理函数
def convert2num(a):
    # 由于文件组织形式，第一行和第四行之后的数据均为数字，需要进行处理
    for i in range(len(a)):
        if i == 0 or i >= 3:
            # strip删掉首尾的引号，split将之从逗号处分割，join再将分割后的部分连接
            a[i] = int(''.join(a[i].split(",")).strip('\"'))
    return a


def dataConvert():
    # 2）转换数值格式，把销量数据中的引号、逗号等处理掉，并转换为数值
    lines = line1.map(lambda x: convert2num(x))
    for item in lines.collect():
        print("{0},{1},{2},{3},{4},{5},{6},{7}".format(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7]))


def typeCnt():
    # 3）统计有多少类型的啤酒？通过distinct去重，count计数
    types = line1.map(lambda x: x[1]).distinct().count()
    print("统计共有" + str(types) + "种类型的啤酒。")


def cntTop5SalesBeer(n):
    # 4）统计哪 5 种啤酒卖得最好？使用lambda表达式
    products = line1.map(lambda x: (x[1], int(x[4]) + int(x[5]) + int(x[6]) + int(x[7]))).reduceByKey(
        lambda a, b: a + b).collect()
    popular_list = sorted(products, key=lambda x: x[1], reverse=True)
    i = 0
    print("卖得最好的5种啤酒如下:")
    while i < n:
        print("销量第{0}名的酒是{1}，其销量为{2}".format(i + 1, popular_list[i][0], popular_list[i][1]))
        i = i + 1


def cntGrowRate():
    # 5）统计哪个销售区域销售的啤酒同比去年增长最快？增长量=4+5+6+7-8行，应包含正负值。
    products = line1.map(lambda x: (x[2], int(x[4]) + int(x[5]) + int(x[6]) + int(x[7]) - int(x[8]))).reduceByKey(
        lambda a, b: a + b).collect()
    popular_list = sorted(products, key=lambda x: x[1], reverse=True)
    print("{0}区域的销量比去年同期增长最快，其增长量为{1}".format(popular_list[0][0], popular_list[0][1]))


def cntSaleAmount():
    # 6）统计每种啤酒的 11 月份销量，即将列表的4 5 6 7行之和作为今年11月的销量
    products = line1.map(lambda x: (x[1], int(x[4]) + int(x[5]) + int(x[6]) + int(x[7]))).reduceByKey(
        lambda a, b: a + b).collect()
    for item in products:
        print("{0}啤酒在今年11月的销量为{1}".format(item[0], item[1]))


def cntTop5SaleAmount():
    # 7）统计啤酒卖得最好的前三个区域的 11 月份销量，组织形式为（地区名称，今年11月销量）
    products = line1.map(lambda x: (x[2], int(x[4]) + int(x[5]) + int(x[6]) + int(x[7]))).reduceByKey(
        lambda a, b: a + b).collect()
    popular_list = sorted(products, key=lambda x: x[1], reverse=True)
    i = 0
    print("啤酒卖得最好的前三个区域的11月份销量如下:")
    while i < 3:
        print("销量第{0}名的是{1}地区，其在11月的销量为{2}".format(i + 1, popular_list[i][0], popular_list[i][1]))
        i = i + 1


# 下面开始执行程序
xlsx2txt("D://CodeWorkSpace//py2_spark//TestSpark//BeerSales.xlsx", "BeerSales.txt")

conf = SparkConf().setMaster("local").setAppName("BeerCnt")
sc = SparkContext(conf = conf)  # 创建spark对象
line = sc.textFile("BeerSales.txt")  # 读入txt文件
line1 = line.map(lambda x: x.split("\t"))  # 基础按行分割，将文本转为RDD

print("1、去除整月销量为 0 的数据")
remove0sales()
print("2、转换数值格式，把销量数据中的引号、逗号等处理掉，并转换为数值")
dataConvert()
print("3、统计有多少类型的啤酒")
typeCnt()
print("4、统计哪 5 种啤酒卖得最好")
cntTop5SalesBeer(5)
print("5、统计哪个销售区域销售的啤酒同比去年增长最快")
cntGrowRate()
print("6、统计每种啤酒的 11 月份销量")
cntSaleAmount()
print("7、统计啤酒卖得最好的前三个区域的 11 月份销量")
cntTop5SaleAmount()

sc.stop()