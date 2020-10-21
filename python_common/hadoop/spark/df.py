import pandas as pd
import json
from pyspark.sql import functions
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler


from python_common.hadoop.spark.udf import vector_indices,to_sparse

class DFUtils():
    def __init__(self,spark):
        self.spark = spark

    def csv_to_parquet(self,src_path,dst_path):
        df = pd.read_csv(src_path)
        spark_df = self.spark.createDataFrame(df)
        spark_df.write.parquet(path=dst_path, mode='overwrite')
        return spark_df

    def pandas_df_to_parquet(self,df,dst_path):
        spark_df = self.spark.createDataFrame(df)
        spark_df.write.parquet(path=dst_path, mode='overwrite')
        return spark_df

    def to_lr_parquet(self,df,dst_path,cate_features,number_featres,label):
        spark_df =self.spark.createDataFrame(df)
        if isinstance(cate_features,str):
            cate_features = [cate_features]
        
        output_cols = [label]

        if len(cate_features) > 0:
            cate_assembler = VectorAssembler(
                inputCols=cate_features,
                outputCol="cate_features")
            spark_df = cate_assembler.transform(spark_df)
            spark_df = spark_df.withColumn('cate_sparse_features', to_sparse('cate_features'))
            output_cols.append('cate_sparse_features')

        if len(number_featres) > 0:
            number_assembler = VectorAssembler(
                inputCols=number_featres,
                outputCol="number_features")
            output_cols.append('number_features')

            spark_df = number_assembler.transform(spark_df)
        
        spark_df = spark_df.select(output_cols)
        spark_df.write.parquet(path=dst_path, mode='overwrite')

