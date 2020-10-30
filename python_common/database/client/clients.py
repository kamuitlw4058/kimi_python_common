
from .db_client import SqlalchemyClient

class Clients:
    def __init__(self, clients_params={}):
        self.clients_params = clients_params
        self.clients = {}

    def register(self,client_params,client_params_key='default'):
        protocol =  client_params.get('protocol',None)
        if protocol is None:
            raise Exception("register client protocol is None!")

        self.clients_params[client_params_key] = client_params
        engine_url = SqlalchemyClient.get_engine_url(**client_params)
        


    def get_client(self, client_params_key='default',client_params=None) -> SqlalchemyClient:
        client = None

        if client_params is not None:
            client_key = SqlalchemyClient.get_engine_url(**client_params)
        elif client_params_key is not None:
            client_params = self.clients_params.get(client_params_key,None)
            if client_params is None:
                return None
            client_key = SqlalchemyClient.get_engine_url(**client_params)


        client = self.clients.get(client_key,None)
        if client is None:
            protocol =  client_params['protocol']
            client = SqlalchemyClient(**client_params)
            self.clients[client_params_key] = client

        return client


clients = Clients()
