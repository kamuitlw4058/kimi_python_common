from python_common.database.client.clients import clients

client = clients.client('hive_xn_bigdata')
df = client.read_sql('show tables')
print(df)