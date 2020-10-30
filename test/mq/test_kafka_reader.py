import time
from python_common.mq.kafka import KafkaClient


client = KafkaClient(['172.16.11.1:9092','172.16.11.252:9092','172.16.11.89:9092'],'adp_test')

client.poll(lambda row: print(row))