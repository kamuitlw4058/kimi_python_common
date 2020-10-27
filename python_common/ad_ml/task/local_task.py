import os

from pyspark.sql import SparkSession
from python_common.ad_ml.datasource.clickhouse_datasource import ClickHouseDataSource
from python_common.ad_ml.preprocess.negative_sampling import NegativeSampling
from python_common.ad_ml.preprocess.features.lr_features import LRFeaturesTransfromer
from python_common.ad_ml.model.trainer.lr_local_trainer import LRLocalTrainer
from python_common.ad_ml.task.base_task import BaseTask
from python_common.utils.logger import getLogger


logger = getLogger(__name__)

class LocalTask(BaseTask):

    def run(self):
        logger.info(self)
        self._spark = SparkSession \
                .builder \
                .appName(f"{self._task_id}_training") \
                .getOrCreate()

                #.config("spark.some.config.option", "some-value") \

        ds =  ClickHouseDataSource(self._table,self._cols,self._filters,self._db_params ,self._spark,expend_opt=self._expend_opts)
        df = ds.dataset()
        df.show()
        train_df = ds.train_dataset()
        test_df = ds.test_dataset()
       
        ns =  NegativeSampling(1,2,'clk','imp')
        train_df =  ns.fit_transform(train_df)


        features_transfromer = LRFeaturesTransfromer(features_opts=self._features_opts)
        train_df =features_transfromer.fit_transform(train_df)
        logger.info('*'*10 ) 
        train_df.show()

        test_df = features_transfromer.transform(test_df)
        test_df.show()
        self.to_parquet(train_df,'train')
        self.to_parquet(test_df,'test')
        features_transfromer.save_features(self._task_dir)
        
        
        trainer = LRLocalTrainer(os.path.join(self._data_dir,'train'),self._model_dir,label=self._label)
        trainer.train()