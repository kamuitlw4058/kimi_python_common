import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError

#producer = KafkaProducer(bootstrap_servers=['172.16.11.1:9092','172.16.11.252:9092','172.16.11.89:9092'],
producer = KafkaProducer(bootstrap_servers=['172.16.11.1:9092','172.16.11.252:9092','172.16.11.89:9092'],

api_version = (0,10),
retries=5)
topic_name = 'adp_test'
partitions = producer.partitions_for(topic_name)
print(f'Topic下分区: {partitions}')
try:
    future = producer.send(topic_name, 'hello aliyun-kafka!')
    future.get()
    print('send message succeed.')
except KafkaError as e:
    print('send message failed.')
    print(e)