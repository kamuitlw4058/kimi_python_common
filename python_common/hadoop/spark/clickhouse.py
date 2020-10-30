
from python_common.database.db_params import DBParams

class ClickHouseNativeClient():
    def __init__(self,db_params):
        if isinstance(db_params,dict):
            self._db_params = DBParams(**db_params)
        elif isinstance(db_params,DBParams):
            self._db_params = db_params
        pass

    def spark_df_to_clickhouse(self,df,table,mode='append'):
        df.write \
            .format("jdbc") \
            .mode(mode) \
            .option("driver", "com.github.housepower.jdbc.ClickHouseDriver") \
            .option("url", f"jdbc:clickhouse://{self._db_params.host}:{self._db_params.port}/{self._db_params.database}")  \
            .option("user", self._db_params.user) \
            .option("password", self._db_params.password) \
            .option("dbtable", table) \
            .option("truncate", "true") \
            .option("batchsize", 10000) \
            .option("isolationLevel", "NONE") \
            .save()

    def clickhouse_to_spark_df(self,spark,query):
        df = spark.read \
        .format("jdbc") \
        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver") \
        .option("url", f"jdbc:clickhouse://{self._db_params.host}:{self._db_params.port}/{self._db_params.database}") \
        .option("user", self._db_params.user) \
        .option("password", self._db_params.password) \
        .option("query",query) \
        .load()
        return df
