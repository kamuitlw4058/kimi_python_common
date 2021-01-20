import abc
import os
import time

import pandas as pd
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import reflection

from python_common.database.client.base_client import BaseClient
from python_common.database.client.base_client import func_elapsed
from python_common.utils.logger import getLogger

logger = getLogger(__name__)

class SqlalchemyClient(BaseClient):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self._default_engine_url = self._db_params.engine_url
        logger.info(f'default engine url:{self._default_engine_url}')
        charset = self._db_params.charset
        create_engine_params = {}

        if self._db_params.protocol == 'hive' and self._db_params.password is not None:
             create_engine_params['connect_args'] ={'auth': 'LDAP'},

        create_engine_params['echo'] = self._echo
        create_engine_params['url'] = self._default_engine_url
        if charset is not None and len(charset) != 0:
            logger.debug(f"SqlalchemyClient charset:{charset} len:{len(charset)}")
            create_engine_params['encoding'] = self._db_params.charset

        self._engine = create_engine(**create_engine_params)
        self.insp = None
    
    @property
    def default_engine_url(self):
        return self._default_engine_url
    
    def user(self):
        return self._db_params.user
    
    def password(self):
        return self._db_params.password

    @property
    def engine(self):
        return self._engine

    @func_elapsed
    def read_value(self, sql,default_value=None,cache_pickle_path=None,use_cache=True,cache_file=True, **kwargs):
        df = self.read_sql(sql,cache_pickle_path=cache_pickle_path,use_cache=use_cache,cache_file=cache_file,**kwargs)
        records_list = df.to_dict('records')
        if len(records_list) != 0 and records_list[0].get('value',None) is not None:
            value =  records_list[0]['value']
        else:
            value = default_value
        return value 

    @func_elapsed
    def read_sql(self, sql,cache_pickle_path=None,use_cache=True,cache_file=True, **kwargs):
        if use_cache and cache_pickle_path is not None and os.path.exists(cache_pickle_path):
            df =  pd.read_pickle(cache_pickle_path)
        else:
            df = pd.read_sql(sql, self._engine)
            if cache_file and cache_pickle_path is not None:
                df.to_pickle(cache_pickle_path)
        return df

    def exec_sql(self, sql):
        self._engine.execute(sql)

    def to_sql(self,df,table,index=False,if_exists='append'):
        df.to_sql(table,self._engine,index=index,if_exists=if_exists)


    def tables(self):
        if self.insp is None:
            self._insp = reflection.Inspector.from_engine(self._engine)
        return self._insp.get_table_names()

    def close(self):
        self._engine.dispose()
