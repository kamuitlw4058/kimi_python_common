import random
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from functools import reduce

from python_common.ad_ml.datasource.base_datasource import DataSource
from python_common.ad_ml.datasource.clickhouse_sql import ClickHouseSQL
from python_common.hadoop.spark import udf
from python_common.utils.logger import getLogger

logger = getLogger(__name__)

class ClickHouseDataSource(DataSource):
    def __init__(self,
                table, 
                features_cols,
                filters,
                db_params,
                spark,
                expend_opt = [],
                expend_dict_opt = [],
                clk_exp = 'notEmpty(Click_Timestamp) = 1',
                clk_name = 'clk',
                imp_exp = '(notEmpty(Click_Timestamp) = 1) OR (notEmpty(Click_Timestamp) = 0)',
                imp_name = 'imp',
                date_col='EventDate',
                ):
        self._table= table
        self._local_table = f'{table}_local'

        self._data_col = date_col
        self._features_cols = features_cols
        self._clk_exp = clk_exp
        self._imp_exp = imp_exp
        self._clk_name = clk_name
        self._imp_name = imp_name
        self._clk_col  =(self._clk_exp,self._clk_name)
        self._imp_col  =(self._imp_exp,self._imp_name)
        self._expend_opt = expend_opt
        self._expend_dict_opt = expend_dict_opt

        self._db_params = db_params
        if isinstance(db_params.host,str):
            hosts = [db_params.host]
        else:
            hosts = db_params.host

        self._hosts = hosts
        self._filters = filters
        self._clickhouse_url_temp = 'clickhouse://{}/{}'
        self._jdbc_clickhouse_url_temp = 'jdbc:clickhouse://{}:8123/{}'
        self._url = self._clickhouse_url_temp.format(random.choice(hosts),db_params.database)
        logger.debug(f'sqlalchemy url:{self._url}')
        self._jdbc_url = self._jdbc_clickhouse_url_temp.format(random.choice(hosts),db_params.database)
        logger.debug(f'jdbc url:{self._jdbc_url}')
        self._engine = create_engine(self._url)
        self._spark = spark
        self._df = None
        self._train_dataset = None
        self._test_dataset = None


    def _col_exp(self,col,op=None):
        ops = '('
        ope = ')'
        if op is None:
            ops = ''
            ope = ''
            op = ''
        if isinstance(col,tuple):
            exp = col[0]
            col_name = col[1]
            return f'{op}{ops}{exp}{ope}  as {col_name}'
        elif isinstance(col,str):
            return f'{op}{ops}{col}{ope}'
        raise ValueError(f'unknonw col type!: {col}')

    def _col_name(self,col):
        if isinstance(col,tuple):
            return col[1]
        elif isinstance(col,str):
            return col
        raise ValueError(f'unknonw col type!: {col}')

    def _col_calc_exp(self,col):
        if isinstance(col,tuple):
            return col[0]
        elif isinstance(col,str):
            return col
        raise ValueError(f'unknonw col type!: {col}')


    def clk_imp(self,df):
        imp_count = df.filter(f'{self._imp_name} = 1').count()
        clk_count =  df.filter(f'{self._clk_name} = 1').count()
        return int(clk_count),int(imp_count)


    def dataset(self):
        if self._df is None:
            cols = ','.join([self._col_exp(i) for i in self._features_cols])
            option_dict ={
                'driver':"ru.yandex.clickhouse.ClickHouseDriver",
                'url':self._jdbc_url,
                'query':f'select {self._data_col},{cols},{self._col_exp(self._imp_col)},{self._col_exp(self._clk_col)} from {self._table}',
            }
            if self._db_params.user:
                option_dict['user']=self._db_params.user
            if self._db_params.password:
                option_dict['password']=self._db_params.password
            logger.debug(option_dict)
            df = self._spark.read.format("jdbc") \
                .options(**option_dict).load()

            df = reduce(lambda d, args: d.withColumn(*args), self._expend_opt, df)

            for input_col,output_col in self._expend_dict_opt:
                df = df.withColumn(output_col, df[input_col].getItem(output_col))

            self._df = df
        
        return  self._df

    def split_dataset(self,df):
        self._train_dataset, self._test_dataset =  df.randomSplit([0.9, 0.1])


    def train_dataset(self):
        if not self._train_dataset:
            df =  self.dataset()
            self.split_dataset(df)

        return self._train_dataset

    def test_dataset(self):
        if not self._test_dataset:
            df =  self.dataset()
            self.split_dataset(df)

        return self._test_dataset
