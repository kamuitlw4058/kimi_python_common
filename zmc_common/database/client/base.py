import abc
import pandas as pd
from sqlalchemy import create_engine


class BaseClient(metaclass=abc.ABCMeta):

    @classmethod
    def get_engine_url(cls,**kwargs):
        engine_params ={}
        for k,v in kwargs.items():
            if k in ['protocol','user','password','host','port','database']:
                engine_params[k] = v
        
        engine_url = '{protocol}://{user}:{password}@{host}:{port}/{database}'.format(**engine_params)
        return engine_url

    def __init__(self,**kwargs):
        engine_url = BaseClient.get_engine_url(**kwargs)
        engine = create_engine(engine_url)
        self.engine_url = engine_url
        self.engine = create_engine(engine_url)
        self.protocol = kwargs['protocol']

    def get_engine(self):
        return self.engine

    def read_sql(self, sql, **kwargs):
        return pd.read_sql(sql, self.engine)

    def exec_sql(self, sql):
        cursor = self.engine.cursor()
        cursor.execute(sql)
        self.engine.commit()
        cursor.close()

    def to_sql(self,df,table,index=False,if_exists='append'):
        df.to_sql(table,self.engine,index=index,if_exists=if_exists)


    @abc.abstractmethod
    def show_table(self):
        pass

    def close(self):
        self.engine.dispose()
