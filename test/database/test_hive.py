# from python_common.database.client.clients import clients

# client = clients.client('hive_xn_bigdata')
# df = client.read_sql('show tables')
# print(df)

from impala.dbapi import connect
conn = connect(host='192.168.66.115', port=10000,auth_mechanism='PLAIN', user='suanfa',
                       password='suanfa123', database='dc_ods_voice')
cur = conn.cursor()
cur.execute('SHOW DATABASES')
data_list=cur.fetchall()
for data in data_list:
    print(data)
