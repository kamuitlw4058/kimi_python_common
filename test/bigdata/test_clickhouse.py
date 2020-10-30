import pandas as pd
import random

from pyspark.sql import SparkSession
from python_common.hadoop.spark.clickhouse import ClickHouseNativeClient

ch_params = {
    'protocol':'clickhouse',
    'user':'chaixiaohui',
    #'user':'default',
    'password':'AAAaaa111!',
    #'password':'',
    'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
    'port':3306,
    'database':'xn_adp'
}



spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
sc =spark.sparkContext
#sc.setLogLevel('debug')
#Logger.getLogger("org").setLevel(Level.INFO)


df = pd.read_csv('data/iris/iris_training.csv')
spark_df = spark.createDataFrame(df)
spark_df.show()
print(spark_df.dtypes)
ch_client = ClickHouseNativeClient(ch_params)
# ch_client.spark_df_to_clickhouse(spark_df,'test')
# print("end save ")
df = ch_client.clickhouse_to_spark_df(spark,'select Id_Nid,EventDate + 1 as EventDate,RequestId,User_Age,Device_Os,Geo_City,Slot_Width from  imp_all')
df.show()
print(df.dtypes)
ch_client.spark_df_to_clickhouse(df,'imp_all')




