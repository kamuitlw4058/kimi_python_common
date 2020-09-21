import time
from kimi_common.mq.kafka import KafkaClient


client = KafkaClient(['10.15.0.106:9092'],'test')

client.poll(lambda row: print(row))