# from python_common.database.client.clients import clients

# client = clients.client('hive_xn_bigdata')
# df = client.read_sql('show tables')
# print(df)

from impala.dbapi import connect
conn = connect(host='192.168.66.115', port=10000)
cur = conn.cursor()
cur.execute('show tables')
data_list=cur.fetchall()
for data in data_list:
    print(data)
