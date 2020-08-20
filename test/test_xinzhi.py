import logging

from zmc_common.weather.xinzhi import WeatherApi
from zmc_common.weather.xinzhi import logger as test_module_logger
from zmc_common.utils.logger import getLogger



if __name__ == "__main__":
    test_module_logger.setLevel(logging.DEBUG)
    weather_client = WeatherApi('PSCzqP5LrvLcTR2wu', 'SI1CUL2GLasinAPHz')


    # city_list = weather_client.load_v3_city_location('data/weather/心知_城市&经纬度 映射表-Zamplus Updated_20200819.xlsx')
    # print(city_list[0])


    ret = weather_client.requests_get('39.90499:116.40529',api_type='hourly')
    print(ret)

    #心知_城市&经纬度 映射表-Zamplus Updated_20200819.xlsx
    
    #https://api.seniverse.com/v3/weather/station/now.json?key=SI1CUL2GLasinAPHz&location=CN-511112&language=zh-Hans&unit=c




