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
        if charset is not None:
            self._engine = create_engine(self._default_engine_url,encoding=self._db_params.charset)
        else:
            self._engine = create_engine(self._default_engine_url)
        self.insp = None
    
    def user(self):
        return self._db_params.user
    
    def password(self):
        return self._db_params.password

    @property
    def engine(self):
        return self._engine

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
        cursor = self._engine.cursor()
        cursor.execute(sql)
        self._engine.commit()
        cursor.close()

    def to_sql(self,df,table,index=False,if_exists='append'):
        df.to_sql(table,self._engine,index=index,if_exists=if_exists)


    def tables(self):
        if self.insp is None:
            self._insp = reflection.Inspector.from_engine(self._engine)
        return self._insp.get_table_names()

    def close(self):
        self._engine.dispose()
