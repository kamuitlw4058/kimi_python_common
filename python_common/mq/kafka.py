from kafka import KafkaConsumer
from kafka import KafkaProducer


class KafkaClient():
    def __init__(self,bootstrap_servers,topic,default_value_series=None,encoding='utf-8',group_id='test'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.producer = None
        self.topic = topic
        self.encoding = encoding
        self.group_id = group_id
        
    def read(self):
        if self.consumer is None:

            print(f'build consumer:{self.bootstrap_servers} topic:{self.topic} group_id:{self.group_id}')
            consumer = KafkaConsumer(group_id=self.group_id, bootstrap_servers=self.bootstrap_servers)
            consumer.subscribe(topics=(self.topic,))
            self.consumer = consumer
            
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
            print(f'build producer:{self.bootstrap_servers} topic:{self.topic}')
            self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,api_version = (0,10))
        if isinstance(msg,str):
            msg = msg.encode(self.encoding)
        
        self.producer.send(self.topic, value=msg, partition=0)
        print("end send")
