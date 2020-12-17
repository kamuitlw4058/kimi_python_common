import os

from python_common.database.client.sqlalchemy_client import SqlalchemyClient
from python_common.database.client.conf import db_params

class SqlalchemyClients:
    def __init__(self, client_params=None,show_func_elapsed=False):
        if client_params is None:
            self.client_params = {}
        else:
            self.client_params = client_params
        self.clients = {}
        self._show_func_elapsed = show_func_elapsed

    def client(self, client_params_key='default',client_params=None,echo=False) -> SqlalchemyClient:
        client = self.clients.get(client_params_key,None)
        if client is None:
            if client_params is  None:
                client_params = self.client_params
                
            client_param = client_params.get(client_params_key,None)
            if client_param is not None:
                if isinstance(client_param,dict):
                    client = SqlalchemyClient(show_func_elapsed=self._show_func_elapsed,echo=echo, **client_param)
                    self.clients[client_params_key] = client
                elif isinstance(client_param,tuple):
                    dev_client_param = None
                    test_client_param = None
                    online_client_param = None
                    if len(client_param) > 0:
                        dev_client_param = client_param[0]

                    if len(client_param) == 2:
                        dev_client_param = client_param[0]
                    elif len(client_param) == 3:
                        test_client_param = client_param[1]
                        online_client_param = client_param[2]
                    
                    db_env =  os.environ.get('DB_ENV','dev')
                    if db_env == 'dev':
                        client = SqlalchemyClient(show_func_elapsed=self._show_func_elapsed,echo=echo, **dev_client_param)
                    elif db_env == 'test':
                        client = SqlalchemyClient(show_func_elapsed=self._show_func_elapsed,echo=echo, **test_client_param)
                    elif db_env == 'online':
                        client = SqlalchemyClient(show_func_elapsed=self._show_func_elapsed,echo=echo, **online_client_param)
                    self.clients[client_params_key] = client

        return client

clients = SqlalchemyClients(db_params,True)
