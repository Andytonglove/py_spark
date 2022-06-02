import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
import pyspark.sql.functions as fun
from pyspark.sql import SQLContext,Window, Row
from itertools import islice

# spark入口
conf = SparkConf().setMaster("local").setAppName("spark-Trajectory-Analysis")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# 建立窗口对象，因源数据所有条目均是同一车，不需要进行分类处理，故partitionBy设为空。
my_window = Window.partitionBy().orderBy("date")


# 通过经纬度与时间差计算速度的函数
def speedCalc(lng1, lat1, lng2, lat2, timelag):
    pi = 3.1415926
    # 角度转弧度
    lng1 = lng1 * pi / 180
    lat1 = lat1 * pi / 180
    lng2 = lng2 * pi / 180
    lat2 = lat2 * pi / 180
    dlon = fun.abs(lng2 - lng1)
    dlat = fun.abs(lat2 - lat1)
    # 计算距离，这里的**代表平方
    angle = fun.sin(dlat / 2) ** 2 + fun.cos(lat1) * fun.cos(lat2) * fun.sin(dlon / 2) ** 2
    distance = 2 * fun.asin(fun.sqrt(angle)) * 6371393  # 地球平均半径，约6371km
    distance = fun.round(distance / 1000, 5)  # 五位小数，单位千米
    speed = distance / timelag  # 距离除以时间差（小时）得到时速
    return speed


# 确定停留点速度阈值函数，这里rate是速度排名(从低到高)百分比
def determine_threshold(df, rate):
    # 若rate不为0, 根据rate确定速度阈值
    if rate != 0.0:
        speed_num = int(df.count() * (1 - rate))
        df.select(max(['speed']))

        df = df.select("speed", fun.rank().over(my_window).alias("rank"))
        rank = df.select("speed").collect()
        rank_list = [row[0] for row in rank]
        threshold = rank_list[speed_num]

        return threshold
    # 若rate等于0, 返回一个给定的速度阈值
    else:
        return 0.4


# 计算速度值函数
def calSpeed(df):
    # 计算速度列，这里将起点和终点速度置为0再进行计算，运用到窗函数
    row_number = df.count()
    df = df.withColumn("speed", fun.when(((fun.col("id") < row_number-1) & (fun.col("id") > 0)),
        speedCalc(fun.lag("latitude", 1).over(my_window), fun.lag("longtitude", 1).over(my_window), 
            df.latitude, df.longtitude, df.timelag)).otherwise(0)) 
    return df


# 计算停留点函数
def calStopPoints(df):
    # 停留点分析
    speed_threshold = determine_threshold(df, 0.01)
    df = df.withColumn("stop_flag", fun.when((fun.lead("speed", 1).over(my_window) < speed_threshold) | \
        (fun.col("speed") < speed_threshold), 1).otherwise(0))
    return df


# 计算加速度函数
def calAcceleration(df):
    # 计算加速度
    df = df.withColumn("acceleration", fun.when((fun.col("id") > 0),
        (df.speed - fun.lag("speed", 1).over(my_window)) / df.timelag).otherwise(0))

    df = df.drop("timelag").drop("unix_time")
    
    df = df.withColumn("flag", fun.when(df.acceleration > 0, 1).otherwise(-1))

    df = df.withColumn("flagChange",
            (fun.col("flag") != fun.lag("flag").over(my_window)).cast("int")
        ).fillna(
            0, subset=["flagChange"]
        )

    # 这里进行bool值取反
    df = df.withColumn("indicator",fun.when((fun.col("id") < df.count()-1),
        (~((fun.lead("flagChange",1).over(my_window)==1) & (fun.col("flagchange")==1)))).otherwise(False))

    return df
    

# 主程序
if __name__ == "__main__":
    # 读取文件的路径
    file_path = "170\\Trajectory\\20080428112704.plt"  # 为简化起见，这里选择170号数据的第一个文件
    output_csv = "result.csv"  # 输出文件的路径，这里输出为csv格式直接是文件夹

    # 文件预处理操作，这里进行一些基础的列计算
    line = sc.textFile(file_path)  # 读取文件
    # 读取了经度、纬度、日期和时间，并将时间和日期合并为一列
    data = line.map(lambda line: line.split(",")).mapPartitionsWithIndex(
        lambda i, test_iter: islice(test_iter, 6, None) if i == 0 else test_iter).map(
            lambda x: {"latitude":float(x[0]), "longtitude":float(x[1]),"date":f"{x[5]} {x[6]}"}).map(
                lambda p: Row(**(p))).toDF()

    # 添加时间戳列
    data = data.withColumn("unix_time", fun.unix_timestamp("date", "yyyy-MM-dd HH:mm:ss")).\
        withColumn("id", fun.monotonically_increasing_id())
    # 计算前后点的时间差，并添加至dataframe中
    data = data.withColumn("timelag", data.unix_time - fun.lag("unix_time", 1).over(my_window))
    data = data.fillna(0, subset=["timelag"])  # 将时间差为空的值赋值0

    # 计算速度
    data = calSpeed(data)
    # 计算停留点
    data = calStopPoints(data)
    # 计算加速度
    data = calAcceleration(data)
    print(data.head(10))
    # 写入文件
    data.coalesce(1).write.mode("append").option("header","true").option("encoding","utf-8").csv(output_csv)
