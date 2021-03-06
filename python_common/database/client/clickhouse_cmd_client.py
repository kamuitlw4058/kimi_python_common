
import random
import pandas as pd
import os
import json

from datetime import datetime

from python_common.database.client.base_client import BaseClient
from python_common.database.client.base_client import func_elapsed
from python_common.utils.shell_utils import run_cli
from python_common.utils.shell_utils import cat
from python_common.utils.logger import getLogger

logger = getLogger(__name__)

class ClickhouseCmdClient(BaseClient):
    def __init__(self,base_dir,**kwargs):
        super().__init__(**kwargs)
        self._base_dir = base_dir
        if not os.path.exists( self._base_dir):
            os.makedirs( self._base_dir)
        self._cache = []
    
    def _build_read_cmd(self,query_path,data_path):
        cmd_list = [f'cat {query_path} | clickhouse-client']
        if self._db_params.host:
            cmd_list.append(f'-h {self._db_params.host}')
        if self._db_params.database:
            cmd_list.append(f'-d {self._db_params.database}')
        if self._db_params.user:
            cmd_list.append(f'-u {self._db_params.user}')
        if self._db_params.port:
            cmd_list.append(f'--port {self._db_params.port}')
        if self._db_params.password:
            cmd_list.append(f'--password {self._db_params.password}')
        cmd_list.append(f'-mn > {data_path}')
        return ' '.join(cmd_list)

    def _build_write_cmd(self,query_path,data_path,table):
        #cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
        cmd_list = [f'cat {data_path} | clickhouse-client']
        if self._db_params.host:
            cmd_list.append(f'-h {self._db_params.host}')
        if self._db_params.database:
            cmd_list.append(f'-d {self._db_params.database}')
        if self._db_params.user:
            cmd_list.append(f'-u {self._db_params.user}')
        if self._db_params.port:
            cmd_list.append(f'--port {self._db_params.port}')
        if self._db_params.password:
            cmd_list.append(f'--password {self._db_params.password}')
        cmd_list.append(f'--query="INSERT INTO {table} FORMAT JSONEachRow"')
        return ' '.join(cmd_list)

    @func_elapsed
    def read_value(self,sql,default_value=None,value_key='value',**kwargs):
        df = self.read_sql(sql,**kwargs)
        records_list = df.to_dict('records')
        if len(records_list) != 0 and records_list[0].get(value_key,None) is not None:
            value =  records_list[0][value_key]
        else:
            value = default_value
        return value 

    @func_elapsed
    def read_sql(self,sql,**kwargs):
        cmd_base_dir = self._base_dir
        running_sql = sql + " FORMAT JSONEachRow"
        dt = datetime.now()
        data_path = cmd_base_dir + "/read_sql_result_{ts:%y%m%d_%H%M%S_%f}.dat".format(ts=dt)
        query_path = cmd_base_dir + "/read_sql_query_{ts:%y%m%d_%H%M%S_%f}.txt".format(ts=dt)

        with open(query_path, 'w') as f:
            f.write(running_sql)

        cmd = self._build_read_cmd(query_path,data_path)
        result = run_cli(cmd)
        data = []
        try:
            with open(data_path,'r') as f:
                d =  f.readline()
                while d:
                    data.append(json.loads(d))
                    d = f.readline()
            result = pd.DataFrame(data)
        except Exception as e:
            logger.info(f" read csv [{data_path}] exeption:{e}")

        if os.path.exists(data_path):
            os.remove(data_path)

        if os.path.exists(query_path):
            os.remove(query_path)

        return result


    def engine(self):
        return 'cmd'


    def exec_sql(self, sql):
        raise Exception('un imp!')

    def write(self,records,table):
        dt = datetime.now()
        data_path = self._base_dir + "/write_sql_data_{ts:%y%m%d_%H%M%S_%f}.json".format(ts=dt)
        query_path = self._base_dir + "/write_sql_query_{ts:%y%m%d_%H%M%S_%f}.sql".format(ts=dt)
        result = None
        try:
            with open(data_path,'w') as f:
                for i in records:
                    f.write(f'{json.dumps(i)}\n')
            cmd = self._build_write_cmd(query_path,data_path,table)
            result = run_cli(cmd)
        finally:
            if os.path.exists(data_path):
                os.remove(data_path)

            if os.path.exists(query_path):
                os.remove(query_path)

        return result

    def cached_to_sql(self,row,table,index=False,if_exists='append',cached_len = 8192, flush=False):
        if len(self._cache) > cached_len or flush:
            cache =  self._cache.copy()
            self._cache.clear()
            return self.write(cache,table)
        else:
            self._cache.append(row)
            return None


    @func_elapsed
    def to_sql(self,df,table,index=False,if_exists='append'):
        if isinstance(df,pd.DataFrame):
            records = df.to_dict("records")
            result = self.write(records,table)

        return result





    def tables(self):
        raise Exception('un imp!')

    def close(self):
        raise Exception('un imp!')