import abc
import os
import time

import pandas as pd
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import reflection

from python_common.utils.logger import getLogger

logger = getLogger(__name__)


class DBClient(metaclass=abc.ABCMeta):

    @classmethod
    def get_engine_url(cls,**kwargs):
        engine_params ={}
        for k,v in kwargs.items():
            if k in ['protocol','user','password','host','port','database']:
                engine_params[k] = v
        
        engine_url = '{protocol}://{user}:{password}@{host}:{port}/{database}'.format(**engine_params)
        return engine_url

    def __init__(self,**kwargs):
        engine_url = DBClient.get_engine_url(**kwargs)
        self.engine_url = engine_url
        logger.info(f'engine url:{engine_url}')

        charset =kwargs.get('charset',None)
        protocol =kwargs.get('protocol',None)
        self._user = kwargs.get('user',None)
        self._password = kwargs.get('password',None)
        self.show_func_elapsed = False
        if charset is not None:
            self.engine = create_engine(engine_url,encoding=charset)
        else:
            self.engine = create_engine(engine_url)
        self.protocol = protocol
        self.insp = None
    
    def user(self):
        return self._user
    
    def password(self):
        return self._password


    def get_engine(self):
        return self.engine

    #@BaseClient.show_func_elapsed
    def read_sql(self, sql,cache_pickle_path=None,use_cache=True,cache_file=True, **kwargs):
        begin_time = time.time()
        if use_cache and cache_pickle_path is not None and os.path.exists(cache_pickle_path):
            df =  pd.read_pickle(cache_pickle_path)
        else:
            df = pd.read_sql(sql, self.engine)
            if cache_file and cache_pickle_path is not None:
                df.to_pickle(cache_pickle_path)
        end_time = time.time()
        run_time = end_time-begin_time
        if self.show_func_elapsed:
            print(f'sql run time:{run_time}')
        return df

    def exec_sql(self, sql):
        cursor = self.engine.cursor()
        cursor.execute(sql)
        self.engine.commit()
        cursor.close()

    def to_sql(self,df,table,index=False,if_exists='append'):
        df.to_sql(table,self.engine,index=index,if_exists=if_exists)


    def tables(self):
        if self.insp is None:
            insp = reflection.Inspector.from_engine(self.engine)
        return insp.get_table_names()

    def close(self):
        self.engine.dispose()
