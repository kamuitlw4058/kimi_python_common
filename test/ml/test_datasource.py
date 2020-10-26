from pyspark.sql import SparkSession

from python_common.ad_ml.datasource.clickhouse_datasource import ClickHouseDataSource
from python_common.ad_ml.datasource.clickhouse_datasource import logger as ds_logger
from python_common.hadoop.spark import udf

from python_common.database.db_params import DBParams

import logging

ds_logger.setLevel(logging.DEBUG)

db_dict ={
'protocol':'clickhouse',
'user':'chaixiaohui',
#'user':'default',
'password':'AAAaaa111!',
#'password':'',
'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
'port':8123,
'database':'xn_adp'
}

db_client =DBParams(**db_dict)


opts = [
    ('weekday', udf.weekday('EventDate')),
    ('is_weekend', udf.is_weekend('EventDate')),
]



spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
# sc = spark.sparkContext
# sc.setLogLevel('DEBUG')
ds =  ClickHouseDataSource('imp_all',['User_Age'],'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',["Device_Os = 'ios'"], db_client,spark,expend_opt=opts)
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
