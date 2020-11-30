import abc
import random

from python_common.utils.logger import getLogger

logger = getLogger(__name__)

class DBParams():

    @classmethod
    def get_engine_url(cls,**kwargs):
        engine_params_dict =DBParams.engine_params(**kwargs)
        engine_url = '{protocol}://{user}:{password}@{host}:{port}/{database}'
        charset = engine_params_dict.get('charset',None) 
        if charset is not None:
            engine_url = f'{engine_url}?charset={charset}'
            
        engine_url =engine_url.format(**engine_params_dict)
        return engine_url

    @classmethod
    def engine_params(cls,**kwargs):
        engine_params_dict ={}
        for k,v in kwargs.items():
            if k in ['protocol','user','password','host','port','database']:
                engine_params_dict[k] = v

        hosts = engine_params_dict.get('hosts',None)
        if not engine_params_dict.get('host',None) and hosts and isinstance(hosts,list):
            engine_params_dict['host'] = random.choice(hosts)
        
        logger.debug(f'engine url params:{engine_params_dict}')
        charset_params = kwargs.get('charset',None)
        if charset_params is not None and len(charset_params) != 0:
            engine_params_dict['charset'] = charset_params
        return engine_params_dict


    def __init__(self,**kwargs):
        self._kwargs = kwargs
        self._hosts =kwargs.get('hosts',None)
        self._host = kwargs.get('host',None)
        if self._host is None:
            if self._hosts is None:
                raise KeyError(" params need host or hosts")
            else:
                self._host = random.choice(self._hosts)

        self._charset =kwargs.get('charset','')
        self._protocol =kwargs.get('protocol',None)
        self._user = kwargs.get('user',None)
        self._password = kwargs.get('password',None)
        if str(self._protocol).lower() == 'clickhouse':
            self._port =  kwargs.get('port',8123)
        else:
            self._port =  kwargs.get('port',None)

        self._database = kwargs.get('database',None)
        self._host = kwargs.get('host',None)

    def to_jdbc_clickhouse_spark_option(self):
        jdbc_clickhouse_url_temp = 'jdbc:clickhouse://{}:{}/{}'
        jdbc_url = jdbc_clickhouse_url_temp.format(self._host,self._port,self._database)
        option_dict ={
            'driver':"ru.yandex.clickhouse.ClickHouseDriver",
            'url':jdbc_url,
           'user':self._user,
           'passwrod':self._password
        }
        logger.debug(option_dict)
        return option_dict

    @property
    def engine_url(self):
        return DBParams.get_engine_url(**DBParams.engine_params(**self._kwargs))
    
    @property
    def host(self):
        if self._hosts and isinstance(self._hosts,list):
            return random.choice(self._hosts)
        return self._host
    
    @property
    def hosts(self):
        return self._hosts

    @property
    def port(self):
        return self._port
        
    @property
    def user(self):
        return self._user
    
    @property
    def password(self):
        return self._password
    
    @property
    def database(self):
        return self._database
    
    @property
    def charset(self):
        return self._charset
    
    
