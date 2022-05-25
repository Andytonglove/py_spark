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

# 先行启动zkserver和kafka

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    '''
    while True:
        s2 = (random.choice(string.ascii_lowercase) for _ in range(2))
        word = ''.join(s2)
        future = producer.send('test', key=word, value='{"name":"caocao1","age":"32","sex":"male"}', partition=0)
        time.sleep(0.1)
    '''
    msg_dict = {
        "operatorId": "test",
        "terminalId": "123",
        "terminalCode": "123",
        "terminalNo": "1",  # 这里传入四个参数
    }
    msg = json.dumps(msg_dict).encode()
    producer.send('tqs-admin-event-1', msg)
    producer.close()