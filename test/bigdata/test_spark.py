import pandas as pd
import random

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

from python_common.hadoop.spark.df import DFUtils
from python_common.hadoop.spark.udf import vector_indices

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df_utils = DFUtils(spark)

df = pd.read_csv('data/iris/iris_training.csv')
df['random_col'] = df.apply(lambda x:  random.randint(0,1), axis=1)
df = df[df.virginica <2]

df_utils.to_lr_parquet(df,'file:///home/wls81/workspace/kimi/kimi_python_common/data/test_parquet',['random_col'],["120", "4", "setosa",'versicolor'],'virginica')
# spark_df =spark.createDataFrame(df)


# cate_assembler = VectorAssembler(
#     inputCols=['random_col'],
#     outputCol="cate_features")

# number_assembler = VectorAssembler(
#     inputCols=["120", "4", "setosa",'versicolor'],
#     outputCol="number_features")

# spark_df = cate_assembler.transform(spark_df)
# spark_df = number_assembler.transform(spark_df)

# spark_df = spark_df.select("cate_features",'number_features', "virginica").filter(spark_df["virginica"] < 2)
# spark_df = spark_df.withColumn('feature_indices', vector_indices('cate_features'))

# v = spark_df.select('cate_features').take(1)[0].cate_features.size
# logger.info('feature vector size = %d',  v.size)
# #spark_df = spark_df.withColumn('feature_indices', to_sparse('features'))
# spark_df.show()
# spark_df.write.parquet(path='file:///home/wls81/workspace/kimi/kimi_python_common/data/test_parquet', mode='overwrite')

