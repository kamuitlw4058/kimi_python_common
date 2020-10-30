from python_common.database.client.sqlalchemy_client import SqlalchemyClient
#import pandas_profiling
import json



if __name__ == "__main__":
    mysql_face_circle_params={
        "host": "gh2outside.mysql.rds.aliyuncs.com",
        "port": 3306,
        "user": "yuanshi_reader",
        "password": "YSdb654123!",
        "database": "face_circle",
        'charset': 'utf8',
        'protocol':'mysql'
    }
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

    db_params = {
        'default':mysql_face_circle_params
    }
    client = SqlalchemyClient(show_func_elapsed=True, **db_dict)

    #client = Clients(clients_params = db_params).get_client()
    #df = client.read_sql("select * from raw_taobao_order where to_date(to_char(order_create_time,'yyyy-mm-dd'),'yyyy-mm-dd') = '2020-06-18'")
    #print(df)

    table_list = client.tables()
    print(client.tables())
    df = client.read_sql("select *from imp_all")
    print(df)

    # df = client.read_sql('select * from face_value_circle_video limit 100')
    # print(df.dtypes)
    # null_count = df.isna().sum().sum()
    # rows_count = len(df)
    # columns_count = len(df.columns)
    # miss_rate = null_count / (rows_count * columns_count )
    # print(null_count)
    # print(rows_count)
    # print(columns_count)
    # print(miss_rate)



    # pd_report =  pandas_profiling.ProfileReport(df)
    # print(repr(pd_report.report.content['body'].content['items']))
    # print(repr(pd_report.report.content['body'].content['items'][0].content['items']))
    # print(repr(pd_report.report.content['body'].content['items'][0].content['items'][0]['items'].content.keys()))
    # print(repr(pd_report.report.content['body'].content['items'][1].content['items'][0]['items'].content.keys()))
    # print(repr(pd_report.report.content['body'].content['items'][1].content['items'][0].keys()))
    # print(repr(pd_report.report.content['body'].content['items'][3].content['items']))


    # j = pd_report.to_json()
    # with open('report.json','w') as f:
    #     f.write(json.dumps(j,ensure_ascii=False))

