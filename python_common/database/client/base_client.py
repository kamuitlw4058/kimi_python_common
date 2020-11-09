import abc
import os
import time
from functools import wraps

import pandas as pd
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import reflection

from python_common.database.db_params import DBParams
from python_common.utils.logger import getLogger

logger = getLogger(__name__)


def func_elapsed(func):
    def wrapper(self, *args, **kwargs):
        begin_time = time.time()
        ret = func(self, *args, **kwargs)
        end_time = time.time()
        run_time = end_time-begin_time
        if self._show_func_elapsed:
            logger.info(f'{func.__name__} sql run time:{run_time}')
        return ret

    return wrapper


class BaseClient(metaclass=abc.ABCMeta):
    def __init__(self,show_func_elapsed =False, **kwargs):
        self._db_params = DBParams(**kwargs)
        self._show_func_elapsed = show_func_elapsed
        self._echo = kwargs.get('echo',False)

    
    @abc.abstractproperty
    def engine(self):
        pass

    @abc.abstractmethod
    def read_sql(self, sql,cache_pickle_path=None,use_cache=True,cache_file=True, **kwargs):
        pass

    @abc.abstractmethod
    def exec_sql(self, sql):
        pass

    @abc.abstractmethod
    def to_sql(self,df,table,index=False,if_exists='append'):
        pass


    @abc.abstractmethod
    def tables(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

