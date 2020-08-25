from zmc_common.database.client.base import BaseClient
from zmc_common.utils.pd_utils import PandasStatics
import numpy as np
import pandas as pd
import pandas_profiling
import json

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime



metadata = MetaData()
shmarathon_news = Table('inner_table_statics', metadata,
     Column('table_name', String, primary_key=True),
     Column('statics', JSONB),
     Column('schedule_datetime', DateTime, primary_key=True),
     Column('zmc_createtime', DateTime),
 )



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
    print(f'start main ...')
    start_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    client = BaseClient(**zmc_postgres_886_params)
    engine = client.get_engine()
    metadata.create_all(engine)

    table_list = client.tables()
    
    for i in table_list:
        print(f'start table:{i} ...')
        df = client.read_sql(f'select * from {i}')
        table_result = []
        print(df.dtypes)
        ps = PandasStatics(df)
        table_result.append(
            {
                'table_name':i,
                'statics':json.dumps(ps.to_json(),ensure_ascii=False,indent=2),
                'zmc_createtime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'schedule_datetime':start_datetime
            }
        )
        output_df = pd.DataFrame(table_result)
        client.to_sql(output_df,'inner_table_statics')


    



