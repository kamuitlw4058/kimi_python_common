from redis import StrictRedis   # 导入redis 模块


class RedisClient():
    def __init__(self,host='localhost',port=6379,decode_responses=True):
        self.redis = StrictRedis(host=host, port=port, decode_responses=decode_responses) 
    
    def set(self,k,v):
        self.redis.set(k,v)
    def get(self,k):
        return self.redis.get(k)
    
    def batch_get(self,k_list,transaction=False):
        pipe = self.redis.pipeline(transaction=transaction)
        for i in k_list:
            pipe.get(i)
        ret = pipe.execute()
        return zip(k_list,ret)

    # r.set('name', 'runoob')  # 设置 name 对应的值
    # print(r['name'])
    # print(r.get('name'))  # 取出键 name 对应的值
    # print(type(r.get('name')))  # 查看类型


