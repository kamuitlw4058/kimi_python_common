from kafka import KafkaConsumer
from kafka import KafkaProducer

# consumer = KafkaConsumer('test_rhj', bootstrap_servers=['xxxx:x'])
# for msg in consumer:
#     recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
#     print recv

# producer = KafkaProducer(bootstrap_servers='xxxx:x')

# msg_dict = {
#     "sleep_time": 10,
#     "db_config": {
#         "database": "test_1",
#         "host": "xxxx",
#         "user": "root",
#         "password": "root"
#     },
#     "table": "msg",
#     "msg": "Hello World"
# }
# msg = json.dumps(msg_dict)
# producer.send('test_rhj', msg, partition=0)
# producer.close()

class KafkaClient():
    def __init__(self,bootstrap_servers,topic,default_value_series=None,encoding='utf-8'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.topic = topic
        self.encoding = encoding
        
    def read(self):
        if self.consumer is None:
            self.consumer = KafkaConsumer(self.topic, bootstrap_servers=self.bootstrap_servers)

        for msg in self.consumer:
            yield msg

    def poll(self,handler,timeout=500):
        while True:
            msg = self.consumer.poll(timeout_ms=timeout) # 从kafka获取消息
            if len(msg) != 0:
                handler(msg)
    
    def write(self,msg):
        if self.producer is None:
            self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)

        self.producer.send(self.topic, value=msg.encode(self.encoding), partition=0)
