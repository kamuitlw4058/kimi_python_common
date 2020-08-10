import requests
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


    def get_url(self,location,api_type='now',base_url=None,key=None):
        api_type = str(api_type).strip()
        api_type_url = self.api_type_mapper.get(str(api_type).strip(),None)
       
        if api_type_url is None:
            raise ValueError(f'unkown api  type:{api_type}')

        if isinstance(location,list):
            location = ','.join([  self.location_mapper(i) for i in location ])
        elif isinstance(location,str):
            location = self.location_mapper(location)
        else:
            raise ValueError(f'unkown location type:{location}')
        
        if base_url is None:
            base_url = self.base_url

        if key is None:
            key = self.key

        url = f'{base_url}/{api_type_url}?key={key}&location={location}&language=zh-Hans&unit=c'
        logger.debug(url)
        return url


    # def get_batch_request(self,url,rows,forecast_type,callback):
    #     request = scrapy.Request(url,callback=callback)
    #     request.meta['forecast_type'] = forecast_type
    #     return request


    #{'results': [{'location': {'id': 'WTW3SJ5ZBJUY', 'name': '上海', 'country': 'CN', 'path': '上海,上海,中国', 'timezone': 'Asia/Shanghai', 'timezone_offset': '+08:00'}, 
    # 'now': {'text': '多云', 'code': '4', 'temperature': '36', 'feels_like': '36', 'pressure': '1000', 'humidity': '50', 'visibility': '19.9', 'wind_direction': '西南', 'wind_direction_degree': '220', 'wind_speed': '2.52', 'wind_scale': '1', 'clouds': '50', 'dew_point': ''}, 
    # 'last_update': '2020-08-10T14:59:00+08:00'}]}
    def requests_get(self, location,api_type='now',base_url=None,key=None):
        url = self.get_url(location,api_type=api_type,base_url=base_url,key=key)
        response = requests.get(url)
        ret = response.json()
        return  ret

