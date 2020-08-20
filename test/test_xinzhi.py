import logging

from zmc_common.weather.xinzhi import WeatherApi
from zmc_common.weather.xinzhi import logger as test_module_logger
from zmc_common.utils.logger import getLogger

    #风向	风向角度（左）	风向角度（右）
# 北	337.51	22.50 
# 东北	22.51	67.50 
# 东	67.51	112.50 
# 东南	112.51	157.50 
# 南	157.51	202.50 
# 西南	202.51	247.50 
# 西	247.51	295.50 
# 西北	295.51	337.50 

def parse_wind_direction(wind_direction):
    wind_direction_index_value = int(float(wind_direction) /22.5)
    wind_direction_index_value =  wind_direction_index_value -1 
    if wind_direction_index_value < 0:
        wind_direction_index_value = 15
    wind_direction_index_value = int(wind_direction_index_value /2)
    print(wind_direction_index_value)
    wind_direction_index = ['东北','东','东南','南','西南','西','西北','北']
    return wind_direction_index[wind_direction_index_value]

if __name__ == "__main__":
    # test_module_logger.setLevel(logging.DEBUG)
    # weather_client = WeatherApi('PSCzqP5LrvLcTR2wu', 'SI1CUL2GLasinAPHz')
    # ret = weather_client.requests_get('CN-511112',api_type='alarms')
    #ret = lambda x:x
    # if isinstance(ret,function):
    #     print("yes")

    #print(ret)
    #print(type(ret))
    print(parse_wind_direction(22.6))
    print(parse_wind_direction(23))
    print(parse_wind_direction(68))
    print(parse_wind_direction(113))
    print(parse_wind_direction(158))
    print(parse_wind_direction(220))
    print(parse_wind_direction(260))
    print(parse_wind_direction(300))
    print(parse_wind_direction(336))
    
    
    #https://api.seniverse.com/v3/weather/station/now.json?key=SI1CUL2GLasinAPHz&location=CN-511112&language=zh-Hans&unit=c




