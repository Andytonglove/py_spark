# -*- coding: utf-8 -*-
import json
import sys
import imp
imp.reload(sys)
sys.setdefaultencoding("utf-8")

import findspark
findspark.init()

#kafka send msg
import string
import random
import time
from kafka import KafkaProducer

# 先行启动zkserver和kafka，保持服务窗口开启
'''
在这个实例中,使用生产者程序每0.1秒生成一个包含2个字母的单词,并写入Kafka的名称为“wordcount-topic”的主题(Topic)内。
Spark的消费者程序通过订阅wordcount-topic,会源源不断收到单词,并且每隔8秒钟对收到的单词进行一次词频统计,
把统计结果输出到Kafka的主题wordcount-result-topic内,同时,通过2个监控程序检查Spark处理的输入和输出结果。
'''

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    # 编写生产者程序
    while True:
        s2 = (random.choice(string.ascii_lowercase) for _ in range(2))
        word = ''.join(s2)
        future = producer.send('test', key=word, value='{"name":"caocao1","age":"32","sex":"male"}', partition=0)
        time.sleep(0.1)
    
    # msg_dict = {
    #     "operatorId": "test",
    #     "terminalId": "123",
    #     "terminalCode": "123",
    #     "terminalNo": "1",  # 这里传入四个参数
    # }
    # msg = json.dumps(msg_dict).encode()
    # producer.send('tqs-admin-event-1', msg)
    # producer.close()