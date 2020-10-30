from kafka import KafkaConsumer
from kafka import KafkaProducer


class KafkaClient():
    def __init__(self,bootstrap_servers,topic,default_value_series=None,encoding='utf-8'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.producer = None
        self.topic = topic
        self.encoding = encoding
        
    def read(self):
        if self.consumer is None:
            self.consumer = KafkaConsumer(self.topic, bootstrap_servers=self.bootstrap_servers)

        for msg in self.consumer:
            yield msg

    def poll(self,handler,timeout=500):
        if self.consumer is None:
            self.consumer = KafkaConsumer(self.topic, bootstrap_servers=self.bootstrap_servers)
        while True:
            msg = self.consumer.poll(timeout_ms=timeout) # 从kafka获取消息
            if len(msg) != 0:
                handler(msg)
    
    def write(self,msg):
        if self.producer is None:
            self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,api_version = (0,10))

        self.producer.send(self.topic, value=msg.encode(self.encoding), partition=0)
