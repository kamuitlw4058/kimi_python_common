import random
import pandas as pd
from sqlalchemy import create_engine

from python_common.database.client.db_client import DBClient
from python_common.ml.datasource.base_datasource import DataSource
from python_common.ml.datasource.clickhouse_sql import ClickHouseSQL
from python_common.utils.logger import getLogger

logger = getLogger(__name__)

class ClickHouseSQLDataSource(DataSource):
    def __init__(self,
                database,
                table, 
                features_cols,
                hosts,
                filters,
                db_client,
                clk_col = ('sum(notEmpty(Click_Timestamp))','clk'),
                imp_col = ('sum(notEmpty(Impression_Timestamp))','imp'),
                date_col='EventDate'):
        self._table= table
        self._local_table = f'{table}_local'

        self._data_col = date_col
        self._features_cols = features_cols
        self._clk_col = clk_col
        self._imp_col = imp_col

        self._db_client = db_client
        self._hosts = hosts
        self._clickhouse_url_temp = 'clickhouse://{}/{}'
        self._url = self._clickhouse_url_temp.format(random.choice(hosts),database)
        self._engine = create_engine(self._url)

    def _col_exp(self,col):
        if isinstance(col,tuple):
            return ' as '.join(col)
        elif isinstance(col,str):
            return col
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


    def get_clk_imp(self, filters):
        self._clk_col
        cols = [
            self._col_exp(self._clk_col),
            self._col_exp(self._imp_col)
        ]
        sql = ClickHouseSQL()
        q = sql.table(self._table).select(cols).where(filters).to_string()
        logger.debug("sql: " + q)

        num = self._db_client.read_sql(q)
        if num.empty:
            clk_num, imp_num = 0, 0
        else:
            clk_num, imp_num = num.clk.sum(), num.imp.sum()

        return clk_num, imp_num
