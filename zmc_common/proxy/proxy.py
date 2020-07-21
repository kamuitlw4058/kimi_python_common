import random

from ..utils.cache_dict_list import CachedDictList

proxy_list = [
    {'host':'61.178.118.86','port':8080,'https':True,'post':True},
    {'host':'103.45.104.83','port':3128,'https':True,'post':True},
    #{'host':'113.226.110.79','port':55317,'https':True,'post':True},
    #{'host':'183.220.145.3','port':8080,'https':True,'post':True},
   # {'host':'61.178.118.86','port':8080,'https':True,'post':True},
   # {'host':'61.178.118.86','port':8080,'https':True,'post':True},
   # {'host':'61.178.118.86','port':8080,'https':True,'post':True},

]

class ProxyList():
    def __init__(self):
        global proxy_list
        self.proxy_list = proxy_list
        self.failed_proxy_dic = CachedDictList('failed_proxy',cache_file_name='failed_proxy.json')
        self.proxy_dic = {}
        for i in self.proxy_list:
            host = i['host']
            port = i['port']
            row_key = f'{host}_{port}'
            if self.failed_proxy_dic.get(row_key) is None:
                self.proxy_dic[row_key] = i
        

    def get_proxy(self,https=True,post=True):
        return self.proxy_dic[list(self.proxy_dic)[random.randint(0, len(self.proxy_dic) - 1)]]

    def remove_proxy(self,host,port):
        row_key = f'{host}_{port}'
        self.failed_proxy_dic.append_cache(row_key,{})
        del self.proxy_dic[row_key]