from zmc_common.database.graphql_client.client import GraphqlClient

client = GraphqlClient('http://localhost:18888/v1/graphql')
client.insert_items('knowledge_weather_forecast',[])