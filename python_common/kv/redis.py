import time
from redis import StrictRedis   # 导入redis 模块
from python_common.utils.logger import getLogger
import random

logger = getLogger(__name__)


class RedisClient():
    def __init__(self,host='localhost',port=6379,decode_responses=True):
        self.redis = StrictRedis(host=host, port=port, decode_responses=decode_responses) 
    
    def set(self,k,v,expire=0,expire_random_rate=0.1):
        self.redis.set(k,v)
        if expire > 0:
            random_second = int(expire * expire_random_rate)
            if random_second >0:
                expire = expire + random.randint(0,random_second)
            self.redis.expire(k,expire)

    def get(self,k,default=None):
        r = self.redis.get(k)
        if r is None:
            return default
        else:
            return r

    def delete(self,k):
        r.delete(k)
    
    def batch_get(self,k_list,transaction=False):
        start =time.time()
        pipe = self.redis.pipeline(transaction=transaction)
        for i in k_list:
            pipe.get(i)
        ret = pipe.execute()
        end =time.time()
        print(f'elasped:{ end - start}')
        return zip(k_list,ret)
    
    def batch_set(self,k_list,v_list,transaction=False,join_str=','):
        pipe = self.redis.pipeline(transaction=transaction)
        for k,v in zip(k_list,v_list):
            if isinstance(v,list):
                v = join_str.join([str(i) for i in v])
            pipe.set(k,v)
        ret = pipe.execute()
        return zip(k_list,ret)

    def batch_set_list(self,k_list,v_list,transaction=False):
        pipe = self.redis.pipeline(transaction=transaction)
        for k,v in zip(k_list,v_list):
            pipe.lset(k,0,v)
        ret = pipe.execute()
        return zip(k_list,ret)
    
    def batch_get_list(self,k_list,transaction=False):
        pipe = self.redis.pipeline(transaction=transaction)
        for k in zip(k_list):
            pipe.lrange(k,0,-1)
        ret = pipe.execute()
        return zip(k_list,ret)

    # r.set('name', 'runoob')  # 设置 name 对应的值
    # print(r['name'])
    # print(r.get('name'))  # 取出键 name 对应的值
    # print(type(r.get('name')))  # 查看类型


