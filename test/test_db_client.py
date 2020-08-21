from zmc_common.database.client.clients import Clients
import pandas_profiling
import json



if __name__ == "__main__":
    zmc_postgres_886_params = {
    "host": "172.22.57.108",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "database": "z_00886",
    'charset': 'utf8',
    'protocol':'postgres'
    }

    dbs_dict = {
        'default':zmc_postgres_886_params
    }

    client = Clients(clients_params = dbs_dict).get_client()
    #df = client.read_sql("select * from raw_taobao_order where to_date(to_char(order_create_time,'yyyy-mm-dd'),'yyyy-mm-dd') = '2020-06-18'")
    #print(df)

    table_list = client.tables()
    print(client.tables())

    df = client.read_sql('select * from inner_qr_code limit 100')
    print(df.dtypes)
    null_count = df.isna().sum().sum()
    rows_count = len(df)
    columns_count = len(df.columns)
    miss_rate = null_count / (rows_count * columns_count )
    print(null_count)
    print(rows_count)
    print(columns_count)
    print(miss_rate)



    pd_report =  pandas_profiling.ProfileReport(df)
    print(repr(pd_report.report.content['body'].content['items']))
    print(repr(pd_report.report.content['body'].content['items'][0].content['items']))
    # print(repr(pd_report.report.content['body'].content['items'][0].content['items'][0]['items'].content.keys()))
    # print(repr(pd_report.report.content['body'].content['items'][1].content['items'][0]['items'].content.keys()))
    # print(repr(pd_report.report.content['body'].content['items'][1].content['items'][0].keys()))
    # print(repr(pd_report.report.content['body'].content['items'][3].content['items']))


    # j = pd_report.to_json()
    # with open('report.json','w') as f:
    #     f.write(json.dumps(j,ensure_ascii=False))

