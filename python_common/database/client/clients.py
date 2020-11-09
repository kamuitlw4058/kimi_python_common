
from python_common.database.client.sqlalchemy_client import SqlalchemyClient
from python_common.database.client.conf import db_params

class SqlalchemyClients:
    def __init__(self, client_params={},show_func_elapsed=False):
        self.client_params = client_params
        self.clients = {}
        self._show_func_elapsed = show_func_elapsed

    def client(self, client_params_key='default',client_params=None,echo=False) -> SqlalchemyClient:
        client = self.clients.get(client_params_key,None)
        if client is None:
            if client_params is not None:
                client_param = client_params.get(client_params_key,None)
            else:
                client_param = self.client_params.get(client_params_key,None)

            if client_param is not None:
                client = SqlalchemyClient(show_func_elapsed=self._show_func_elapsed,echo=echo, **client_param)
                self.clients[client_params_key] = client

        return client

clients = SqlalchemyClients(db_params,True)
