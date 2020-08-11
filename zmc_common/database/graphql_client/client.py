from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

from zmc_common.utils.logger import getLogger

logger = getLogger(__name__)

class GraphqlClient:
    def __init__(self,url):
        sample_transport=RequestsHTTPTransport(
            url=url,
            verify=False,
            retries=3,
        )

        self.client = Client(
            transport=sample_transport,
            fetch_schema_from_transport=True,
        )

        self.cached_dict = {}


    def _get_on_conflict(self,pkey_name,update_columns):
        s =  ', on_conflict:' + \
            '{' + \
            f'constraint: {pkey_name}, update_columns: {update_columns}'+ \
            '}' 
        return s

    def append(self,table_name,item_dict):
        item_list =  self.cached_dict.get(table_name,None)
        if item_list is None:
            item_list = []
        item_list.append(item_dict)
        self.cached_dict[table_name] = item_list

    def insert(self,table_name,limit_size=0):
        item_list =  self.cached_dict.get(table_name,None)
        if item_list is not None and len(item_list) > limit_size:
            items= item_list.copy()
            self.cached_dict[table_name] = []
            self.insert_items(table_name,items)

    def insert_items(self,table_name,items,on_conflict=False,update_columns='null'):
        item_name = f'{table_name}_insert_input'  #knowledge_weather_forecast_insert_input
        func_name = f'insert_{table_name}' #insert_knowledge_weather_forecast
        pkey_name = f'{table_name}_pkey'
        if on_conflict:
            on_onflict_str = self._get_on_conflict(pkey_name,update_columns)
        else:
            on_onflict_str = ''

        gql_str = f'mutation MyMutation($objects: [{item_name}!]!) ' + \
            '{ '+ \
            f'{func_name}(objects: $objects' + \
            on_onflict_str + \
            ')' + \
            '''
                {
                    affected_rows
                }
            }
            '''
        query = gql(gql_str)
        try:
            self.client.execute(query,variable_values = {
                "objects":items
            })
            logger.info(f"insert items:{len(items)}")
        except Exception as e:
            logger.error("execute failed!",e)

