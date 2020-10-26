import abc

class DBParams():

    @classmethod
    def get_engine_url(cls,**kwargs):
        engine_params ={}
        for k,v in kwargs.items():
            if k in ['protocol','user','password','host','port','database']:
                engine_params[k] = v
        
        engine_url = '{protocol}://{user}:{password}@{host}:{port}/{database}'.format(**engine_params)
        return engine_url

    def __init__(self,**kwargs):
        self._kwargs = kwargs
        self._charset =kwargs.get('charset',None)
        self._protocol =kwargs.get('protocol',None)
        self._user = kwargs.get('user',None)
        self._password = kwargs.get('password',None)
        if str(self._protocol).lower() == 'clickhouse':
            self._port =  kwargs.get('port',8123)
        else:
            self._port =  kwargs.get('port',None)

        self._database = kwargs.get('database',None)
        self._host = kwargs.get('host',None)

    @property
    def engine_url(self):
        return DBParams.get_engine_url(**self._kwargs)
    
    @property
    def host(self):
        return self._host

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
    def  charset(self):
        return self.charset
    
    
