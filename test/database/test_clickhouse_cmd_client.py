from python_common.database.client.clickhouse_cmd_client import ClickhouseCmdClient
import json


if __name__ == "__main__":

    db_dict ={
        'protocol':'clickhouse',
        'user':'chaixiaohui',
        'password':'AAAaaa111!',
        'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
        'port':3306,
        'database':'xn_adp'
    }

    client = ClickhouseCmdClient('data/test_database', show_func_elapsed=True, **db_dict)
    df = client.read_sql("select Id_Nid,EventDate + 2 as EventDate,RequestId,User_Age,Device_Os,Geo_City,Slot_Width,Click_Timestamp from  imp_all")
    print(df)
    client.to_sql(df,'imp_all')

    
