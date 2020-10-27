import time 
import os
import abc
from pyspark.sql import SparkSession

from python_common.hadoop.spark.udf import to_sparse
from python_common.utils.logger import getLogger


logger = getLogger(__name__)

class BaseTask(metaclass=abc.ABCMeta):
    def __init__(self,
                task_id,
                table,
                filters,
                cols,
                label,
                expend_opts,
                features_opts,
                task_base_dir,
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
        self._task_base_dir = task_base_dir
        self._data_dir = data_dir
        self._model_dir = model_dir
        self._label = label
        self._spark = None
        
    @property
    def task_id(self):
        return self._task_id

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
    def task_dir(self):
        return self._task_base_dir


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

    @abc.abstractclassmethod
    def run(self):
        pass

    @classmethod
    def run(cls,task):
        task.run()

    def __str__(self):
        return f'{self._task_id} test task'