from python_common.kv.redis import RedisClient
import time

redis = RedisClient(host='192.168.3.64')
key1 = 'key1'
v = redis.get(key1)
print(v)
redis.set(key1,'1')
v = redis.get(key1)
print(v)
redis.set(key1,'3',10)
v = redis.get(key1)
print(v)
redis.delete(key1)
v = redis.get(key1)
print(v)
redis.set(key1,'2',10)
v = redis.get(key1)
print(v)
time.sleep(11)
v = redis.get(key1)
print(v)
