import time
import random
import json

from python_common.mq.kafka import KafkaClient

#172.16.11.1:9092,172.16.11.252:9092,172.16.11.89:9092

client = KafkaClient(['172.16.11.1:9092','172.16.11.252:9092','172.16.11.89:9092'],'adp_test')
i = 0
print('enter loop')
while True:
    count = random.randint(1,1)
    for r in range(count):
        d = {
            'test':f"{i}:{r}",
        }
        print(d)
        client.write(json.dumps(d))
        print('end')
        i+=1

    time.sleep(10)

