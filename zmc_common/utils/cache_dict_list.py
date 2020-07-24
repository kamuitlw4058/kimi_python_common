import os
import json

from ..utils.logger import getLogger
logger = getLogger(__name__)

class CachedDictList():
    def __init__(self,cache_keyword, cache_file_name='cache_dic.json',  cache_dir='.cache'):
        self.cache_dic = None
        self.cache_file_name = cache_file_name
        self.cache_dir = cache_dir
        self.cache_path = f'{self.cache_dir}/{self.cache_file_name}'
        self.cache_keyword  = cache_keyword
        self.load_cache()

    def get_keys(self):
        return list(self.cache_dic)

    def load_cache( self):
        logger.debug("location load cache... ")
        self.cache_dic = {}
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        if os.path.exists(self.cache_path):
            with open(f'{self.cache_dir}/{self.cache_file_name}', 'r') as f:
                rows = f.readlines()
                for i in rows:
                    try:
                        row_dict = json.loads(i)
                        self.cache_dic[row_dict[self.cache_keyword]] = row_dict
                    except Exception as e:
                        logger.debug(e)
                        pass

    def append_cache(self,row_key, row_dic={}):
        if not isinstance(row_dic,dict):
            raise Exception(f'append cache need dic. real:{row_dic}!')
    
        try:
            row_dic[self.cache_keyword] = row_key
            self.cache_dic[row_key] = row_dic
            with open(self.cache_path, 'a') as f:
                f.write(json.dumps(row_dic, ensure_ascii=False) + '\n')
        except Exception as e:
            logger.debug(e)
            pass


    def get(self,row_key):
        return self.cache_dic.get(row_key, None)
