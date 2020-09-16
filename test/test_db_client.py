from kimi_common.database.client.clients import Clients
import pandas_profiling
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

    db_params = {
        'default':mysql_face_circle_params
    }

    client = Clients(clients_params = db_params).get_client()
    #df = client.read_sql("select * from raw_taobao_order where to_date(to_char(order_create_time,'yyyy-mm-dd'),'yyyy-mm-dd') = '2020-06-18'")
    #print(df)

    table_list = client.tables()
    print(client.tables())

    df = client.read_sql('select * from face_value_circle_video limit 100')
    print(df.dtypes)
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

