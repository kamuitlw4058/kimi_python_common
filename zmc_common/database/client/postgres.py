
from .base import BaseClient


class PostgresClient(BaseClient):
    def __init__(self, **kwargs):
        protocol =  kwargs.get('protocol',None)
        if protocol != 'postgres':
            raise Exception("Wrong Database protocol")
        kwargs['protocol'] = 'postgres'
        super().__init__(**kwargs)

    def show_table(self):
        raise NotImplementedError

    

