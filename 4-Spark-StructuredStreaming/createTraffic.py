# -*- coding: utf-8 -*-
import sys
import imp
imp.reload(sys)
sys.setdefaultencoding("utf-8")

import findspark
findspark.init()

import json
import time
from kafka import KafkaProducer
import pandas as pd
import math

'''
编写车辆位置数据模拟生成程序。
从车辆坐标文件中,获取车辆轨迹信息,然后定时将数据发送到指定Kafka的消息队列的车辆位置topic中。
'''

def dist(lat1, lon1, lat2, lon2):
    # 计算两点间距离 wgs84 单位:m
    # 传入参数为第一点和第二点的纬度和经度
    lat1 = float(lat1)
    lat2 = float(lat2)
    lon1 = float(lon1)
    lon2 = float(lon2)
    R = 6371
    dLat = (lat2 - lat1) * math.pi / 180.0
    dLon = (lon2 - lon1) * math.pi / 180.0

    a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(lat1 * math.pi / 180.0) * math.cos(
        lat2 * math.pi / 180.0) * math.sin(dLon / 2) * math.sin(dLon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    dist = R * c
    return dist * 1000


if __name__ == "__main__":
    # 编写生产者程序
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    path = "20081023025304.plt"
    # 把plt文件读成spark dataframe，注意文件结构
    pandas_df = pd.read_csv(path, header=None, skiprows=6, usecols=[0, 1, 3, 4, 5, 6],
                        names=['Latitude', 'Longitude', 'Altitude', 'Day_from1899', 'Date', 'Time'])
    for i in range(pandas_df.shape[0] - 2):
        # 发送车辆轨迹消息
        msg_info = {
            "lat1" : pandas_df.loc[i + 1][0], 
            "lon1" : pandas_df.loc[i + 1][1], 
            "lat2" : pandas_df.loc[i][0], 
            "lon2" : pandas_df.loc[i][1],
            "time1": pandas_df.loc[i + 1][3],
            "time2": pandas_df.loc[i][3],
            # 计算两点之间距离
            "dist" : dist(pandas_df.loc[i + 1][0], pandas_df.loc[i + 1][1], pandas_df.loc[i][0], pandas_df.loc[i][1])
        }
        msg = json.dumps(msg_info).encode('utf-8')
        producer.send('traffic', msg)
        print("send message to traffic successfully:", msg)
        time.sleep(0.2)  # 每隔固定时间发送一次消息
    producer.close()