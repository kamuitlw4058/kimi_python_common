import time 
import os
from pyspark.sql import SparkSession

from python_common.ad_ml.datasource.clickhouse_datasource import ClickHouseDataSource
from python_common.ad_ml.preprocess import NegativeSampling
from python_common.ad_ml.features.lr_features import LRFeaturesTransfromer
from python_common.ad_ml.model.trainer.lr_local_trainer import LRLocalTrainer
from python_common.hadoop.spark.udf import to_sparse
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
                label,
                expend_opts,
                features_opts,
                data_dir,
                model_dir,
                db_params,):
        self._task_id =task_id
        self._table = table
        self._filters = filters
        self._expend_opts = expend_opts
        self._cols = cols
        self._features_opts = features_opts
        self._db_params = db_params
        self._data_dir = data_dir
        self._model_dir = model_dir
        self._label = label
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

    @property
    def label(self):
        return self._label

    @property
    def data_dir(self):
        return self._data_dir

    @property
    def model_dir(self):
        return self._model_dir

    def set_spark(self,spark):
        self._spark = spark

    
    def features_list(self):
        onehot_features = []
        number_features = []
        multi_onehot_features = []
        for k,v in self._features_opts.items():
            if 'number' in v:
                number_features.append(k)
            elif 'onehot' in v:
                onehot_features.append(k)
            elif 'multi_onehot' in v:
                multi_onehot_features.append(k)
        return number_features,onehot_features,multi_onehot_features
    
    
    def to_parquet(self,df,subdir):
        df = df.withColumn('onehot_sparse_features', to_sparse('onehot_features'))
        df.write.parquet(path=os.path.join(self._data_dir,subdir), mode='overwrite')


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
       
        ns =  NegativeSampling(1,2,'clk','imp')
        train_df =  ns.fit_transform(train_df)
        test_df = ns.transform(test_df)

        number_features,onehot_features,multi_onehot_features = task.features_list()


        features_transfromer = LRFeaturesTransfromer(onehot_features=onehot_features,number_features=number_features,multi_onehot_features=multi_onehot_features)
        train_df =features_transfromer.fit_transform(train_df)
        logger.info('*'*10 ) 
        train_df.show()

        test_df = features_transfromer.transform(test_df)
        task.to_parquet(train_df,'train')
        task.to_parquet(test_df,'test')
        
        time.sleep(3)

        
        trainer = LRLocalTrainer(os.path.join(task.data_dir,'train'),task.model_dir,label=task.label)
        trainer.train()

        

    def __str__(self):
        return f'{self._task_id} test task'