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


    pd_report =  pandas_profiling.ProfileReport(client.read_sql('select * from inner_qr_code') )
    j = pd_report.to_json()
    with open('report.json','w') as f:
        f.write(json.dumps(j,ensure_ascii=False))

