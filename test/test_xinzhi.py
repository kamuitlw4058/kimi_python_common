import logging

from zmc_common.weather.xinzhi import WeatherApi
from zmc_common.weather.xinzhi import logger as test_module_logger
from zmc_common.utils.logger import getLogger

if __name__ == "__main__":
    test_module_logger.setLevel(logging.DEBUG)
    weather_client = WeatherApi('SI1CUL2GLasinAPHz')
    ret = weather_client.requests_get('CN-310100',base_url='https://api.seniverse.com/v3/weather/station')
    #ret = lambda x:x
    # if isinstance(ret,function):
    #     print("yes")

    print(ret)
    #print(type(ret))
    
    
