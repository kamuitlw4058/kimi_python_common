from python_common.ml.datasource.sql_datasource import ClickHouseSQLDataSource
from python_common.database.client.base import DBClient

db_dict ={
'protocol':'clickhouse',
'user':'chaixiaohui',
#'user':'default',
'password':'AAAaaa111!',
#'password':'',
'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
'port':8123,
'database':'nx_adp'
}

db_client =DBClient(**db_dict)

ds =  ClickHouseSQLDataSource('xn_adp','imp_all',['User_Age'],'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',[], db_client)
ds.get_clk_imp([])