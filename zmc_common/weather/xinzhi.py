import requests
import scrapy

from ..utils.logger import getLogger
logger = getLogger(__name__)

class WeatherApi():
    api_type_mapper ={
        'now':'now.json'
    }

    #https://api.seniverse.com/v3/weather/station/now.json?key=your_api_key&location=CN-511112
    def __init__(self,key,base_url ='https://api.seniverse.com/v3/weather',location_mapper=None):
        self.key= key
        self.base_url=base_url
        if location_mapper is None:
            self.location_mapper = lambda x:x
        else:
            self.location_mapper = location_mapper
    
    def get_location(self,location,location_mapper=None):
        if location_mapper is None:
            location_mapper = self.location_mapper

        if isinstance(location,list):
            location = ','.join([  location_mapper(i) for i in location ])
        elif isinstance(location,str):
            location = location_mapper(location)
        else:
            raise ValueError(f'unkown location type:{type(location)} value: {location}')

        return location


    def get_url(self,location,api_type='now',base_url=None,key=None):
        api_type = str(api_type).strip()
        api_type_url = self.api_type_mapper.get(str(api_type).strip(),None)
       
        if api_type_url is None:
            raise ValueError(f'unkown api  type:{api_type}')


        if base_url is None:
            base_url = self.base_url

        if key is None:
            key = self.key

        location = self.get_location(location)

        url = f'{base_url}/{api_type_url}?key={key}&location={location}&language=zh-Hans&unit=c'
        logger.debug(url)
        return url


    def scrapy_request(self,location,callback,api_type='now',base_url=None,key=None,meta=None):
        url = self.get_url(location,api_type=api_type,base_url=base_url,key=key)
        request = scrapy.Request(url,callback=callback)
        if meta is not None:
            request.meta.update(meta)
        request.meta['api_type'] = api_type
        return request


    #{'results': [{'location': {'id': 'CN-310101', 'name': '黄浦区', 'country': 'CN', 'path': '黄浦区,上海市,中国', 'timezone': 'Asia/Shanghai', 'timezone_offset': '+08:00'}, 
    # 'data': {'code': '13', 'humidity': '84', 'precip': '0.5', 'pressure': '1002.7', 'temperature': '27', 'time': '2020-08-10T17:00:00+08:00', 'visibility': '5.9', 'wind_direction': '东', 'wind_direction_degree': '102', 'wind_scale': '1', 'wind_speed': '3.96', 'text': '小雨'},
    #  'last_update': '2020-08-10T17:17:34+08:00'}]}
    def requests_get(self, location,api_type='now',base_url=None,key=None):
        url = self.get_url(location,api_type=api_type,base_url=base_url,key=key)
        response = requests.get(url)
        ret = response.json()
        return  ret

