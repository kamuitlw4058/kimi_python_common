import pandas as pd

from python_common.utils.logger import getLogger
logger = getLogger(__name__)

from python_common.database.client.clickhouse_cmd_client import ClickhouseCmdClient


class PandasProcessor():
    def __init__(self,table):
        self._table = table
    
    def process(self,data_list):
        df = pd.DataFrame(data_list)
        print(df)

class ClickhouseCmdProcessor():
    def __init__(self,table,db_params):
        self._table = table
        self._db_params = db_params
        self._client = ClickhouseCmdClient('data/ch_tmp',** db_params)
    
    def process(self,data_list):
        df = pd.DataFrame(data_list)
        self._client.to_sql(df,self._table)
        

class CacheItemHandler():
    def __init__(self,item_class,processor,cache_size=8192):
        self._item_class = item_class,
        self._cache_size = cache_size
        self._processor = processor
        self._cache = []

    @property
    def item_class(self):
        return self._item_class

    def append(self,item):
        self._cache.append(item)

    def process(self,flush=False):
        if self._processor is not None and (flush or len(self._cache) > self._cache_size):
            data_list = self._cache.copy()
            self._cache.clear()
            logger.info(f'process data len:{len(data_list)}')
            self._processor.process(data_list)


    def finish(self):
        self.process(flush=True)
        logger.info(f"end finish flush items")


class CachePipline():
    def __init__(self,item_handlers):
        self._item_handlers = item_handlers
    
    def process_item(self,pipline,item,spider):
        for i in self._item_handlers:
            if isinstance(item,i.item_class):
                logger.info(f'process_item {i.item_class}')
                i.append(item)
                i.process()


    def close_spider(self,pipline, spider):
        for i in self._item_handlers:
            i.finish()
                