from python_common.kv.redis import RedisClient
import time

redis = RedisClient(host='192.168.3.64')
key1 = 'key1'
v = redis.get(key1)
print(v)
redis.set(key1,'1')
v = redis.get(key1)
print(v)
redis.set(key1,'2',3)
v = redis.get(key1)
print(v)
time.sleep(5)
v = redis.get(key1)
print(v)
