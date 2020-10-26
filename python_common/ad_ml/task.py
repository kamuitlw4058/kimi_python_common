
from pyspark.sql import SparkSession

from python_common.ad_ml.datasource.clickhouse_datasource import ClickHouseDataSource
from python_common.ad_ml.datasource.clickhouse_datasource import logger as ds_logger
from python_common.hadoop.spark import udf
from python_common.utils.logger import getLogger

logger = getLogger(__name__)

class Task():
    def __init__(self,
                task_id,
                table,
                filters,
                cols,
                expend_opts,
                features_opts,
                db_params,):
        self._task_id =task_id
        self._table = table
        self._filters = filters
        self._expend_opts = expend_opts
        self._cols = cols
        self._features_opts = features_opts
        self._db_params = db_params
        self._spark = None
    
    @property
    def table(self):
        return self._table
    @property
    def cols(self):
        return self._cols

    @property
    def features_opts(self):
        return self._features_opts

    @property
    def filters(self):
        return self._filters

    @property
    def expend_opts(self):
        return self._expend_opts

    @property
    def db_params(self):
        return self._db_params


    def set_spark(self,spark):
        self._spark = spark
        

    @classmethod
    def run(cls,task):
        logger.info(task)
        spark = SparkSession \
                .builder \
                .appName("Python Spark SQL basic example") \
                .config("spark.some.config.option", "some-value") \
                .getOrCreate()

        task.set_spark(spark)

        ds =  ClickHouseDataSource(task.table,task.cols,task.filters,task.db_params ,spark,expend_opt=task.expend_opts)
        df = ds.dataset()
        df.show()
        train_df = ds.train_dataset()
        test_df = ds.test_dataset()
        train_df.show()
        test_df.show()
        imp,clk = ds.clk_imp(train_df)
        print(imp,clk)
        imp,clk = ds.clk_imp(test_df)
        print(imp,clk)
        
        pass

    def __str__(self):
        return f'{self._task_id} test task'