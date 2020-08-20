import requests
import scrapy
import jsonpath
import pandas as pd

from ..utils.logger import getLogger
from ..crypto.base import Crypto
from ..location.gaode import Location

logger = getLogger(__name__)

def sig_url(fields,public_key,private_key,query_location,crypto):
    d = {
    'fields':fields,
    'public_key':public_key,
    'locations':query_location,
    }
    params =crypto.url_hmac(d)
    return params


class WeatherItem(scrapy.Item):
    api_type = scrapy.Field()

    province = scrapy.Field()
    city = scrapy.Field()
    district = scrapy.Field()
    city_short = scrapy.Field()
    district_short = scrapy.Field()
    city_district_short = scrapy.Field()
    
    code = scrapy.Field()
    relative_humidity = scrapy.Field()
    pressure = scrapy.Field()
    wind_scale = scrapy.Field()
    wind_direction = scrapy.Field()
    temperature = scrapy.Field()
    weather = scrapy.Field()
    rainfall = scrapy.Field()
    visibility = scrapy.Field()
    wind_direction_cn = scrapy.Field()
    wind_speed = scrapy.Field()
    lng = scrapy.Field()
    lat = scrapy.Field()

    time = scrapy.Field()
    update_time = scrapy.Field()
    schedule_datetime = scrapy.Field()
    
    orig =  scrapy.Field()


class WeatherAlarmItem(scrapy.Item):

    province = scrapy.Field()
    city = scrapy.Field()
    district = scrapy.Field()
    city_short = scrapy.Field()
    district_short = scrapy.Field()
    lng = scrapy.Field()
    lat = scrapy.Field()
    
    title = scrapy.Field() 
    level = scrapy.Field()
    status = scrapy.Field()
    description = scrapy.Field()

    time = scrapy.Field()
    schedule_datetime = scrapy.Field()
    orig =  scrapy.Field()



class WeatherApi():

    def __init__(self,public_key,private_key,location_mapper=None):
        self.public_key= public_key
        self.private_key= private_key
        self.crypto = Crypto(private_key=private_key,public_key=public_key)
        if location_mapper is None:
            self.location_mapper = lambda x:x
        else:
            self.location_mapper = location_mapper
        self.location_utils = Location()
        self.v3_city_location_table_df = None

    api_type_mapper ={
        'now':('https://api.seniverse.com/v3/weather/station/now.json',None,lambda fields,public_key,private_key,query_location,crypto:f'key={private_key}&location={query_location}&language=zh-Hans&unit=c'),
        'hourly':('https://api.seniverse.com/v4','weather_hourly_1h',sig_url),
        'hourly3h':('https://api.seniverse.com/v4','weather_hourly_3h',sig_url),
        'extended':('https://api.seniverse.com/v3/pro/weather/grid/extended.json',None,lambda fields,public_key,private_key,query_location,crypto:f'key={private_key}&location={query_location}'),
        'alarms':('https://api.seniverse.com/v3/weather/alarm.json',None,lambda fields,public_key,private_key,query_location,crypto:f'key={private_key}'),
    }

    def load_v3_city_location(self,path,sheet_name='城市&经纬度 映射表'):
        df = pd.read_excel(path,sheet_name=sheet_name)
        self.v3_city_location_table_df = df
        df = df[['Prov','City','District','lat','lng','Starbucks']]
        df = df[df['Starbucks'] == 'Y']
        city_list =  df.to_dict("records")
        logger.info(len(city_list))
        logger.info(df.groupby(['Starbucks']).count())
        return  city_list
        


    
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


    def get_url(self,location,api_type='now',location_type=None):
        api_type = str(api_type).strip()
        base_url,fields,params_func = self.api_type_mapper.get(str(api_type).strip(),(None,None))
        if base_url is None:
            raise ValueError(f'unkown api type:{api_type}')

        query_location = self.get_location(location)
        url = f'{base_url}?{params_func(fields,self.public_key,self.private_key,query_location,self.crypto)}'
        logger.debug(url)
        return url


    def scrapy_request(self,location,callback,api_type='now',meta=None):
        url = self.get_url(location,api_type=api_type)
        request = scrapy.Request(url,callback=callback)
        if meta is not None:
            request.meta.update(meta)
        request.meta['api_type'] = api_type
        return request
    
    
    #{'results': [{'location': {'id': 'CN-310101', 'name': '黄浦区', 'country': 'CN', 'path': '黄浦区,上海市,中国', 'timezone': 'Asia/Shanghai', 'timezone_offset': '+08:00'}, 
    # 'data': {'code': '13', 'humidity': '84', 'precip': '0.5', 'pressure': '1002.7', 'temperature': '27', 'time': '2020-08-10T17:00:00+08:00', 'visibility': '5.9', 'wind_direction': '东', 'wind_direction_degree': '102', 'wind_scale': '1', 'wind_speed': '3.96', 'text': '小雨'},
    #  'last_update': '2020-08-10T17:17:34+08:00'}]}
    def requests_get(self, location,api_type='now'):
        url = self.get_url(location,api_type=api_type)
        response = requests.get(url)
        ret = response.json()
        return  ret
    

    
    #1000	 晴	0	晴
    #         1	晴
    # 1001	 少云	-	-
    # 1002	 多云	4	多云
    #         5	晴间多云
    #         6	晴间多云
    #         7	大部多云
    #         8	大部多云
    # 1003	 阴	9	阴
    # 2001	 小雨	13	 小雨
    # 2003	 中雨	14	 中雨
    # 2005	 大雨	15	 大雨
    # 2007	 暴雨	16	 暴雨
    # 2009	 大暴雨	17	 大暴雨
    # 2011	 特大暴雨	18	 特大暴雨
    # 3001	 小雪	22	 小雪
    # 3003	 中雪	23	 中雪
    # 3005	 大雪	24	 大雪
    # 3007	 暴雪	25	 暴雪
    # 3022	 雨夹雪	20	 雨夹雪
    # 4009	 大风	32	风
    #         33	大风
    # 2025	 冻雨	19	 冻雨
    # 2020	 阵雨	10	阵雨
    #         11	雷阵雨
    # 3020	 阵雪	21	 阵雪
    # 9020	 冰雹	12	雷阵雨伴有冰雹
    def weather_code_v3_v4_mapper(self,v3_code):
        v3_code = str(v3_code)
        mapper_dict = {
            '0':'1000',
            '1':'1000',
            '4':'1002',
            '5':'1002',
            '6':'1002',
            '7':'1002',
            '8':'1002',
            '9':'1003',
            '13':'2001',
            '14':'2003',
            '15':'2005',
            '16':'2007',
            '17':'2009',
            '18':'2011',
            '22':'3001',
            '23':'3003',
            '24':'3005',
            '25':'3007',
            '20':'3022',
            '32':'4009',
            '33':'4009',
            '19':'2025',
            '10':'2020',
            '11':'2020',
            '21':'3020',
            '12':'9020',
        }

        return mapper_dict.get(v3_code,'1000')

    def weather_code_v4_text(self,v4_code):
        v4_code = str(v4_code)
        mapper_dict ={
            '1000':'晴',
            '1000':'晴',
            '1001':'少云',
            '1002':'多云',
            '1002':'多云',
            '1002':'多云',
            '1002':'多云',
            '1002':'多云',
            '1003':'阴',
            '2001':'小雨',
            '2003':'中雨',
            '2005':'大雨',
            '2007':'暴雨',
            '2009':'大暴雨',
            '2011':'特大暴雨',
            '3001':'小雪',
            '3003':'中雪',
            '3005':'大雪',
            '3007':'暴雪',
            '3022':'雨夹雪',
            '4009':'大风',
            '4009':'大风',
            '2025':'冻雨',
            '2020':'阵雨',
            '2020':'阵雨',
            '3020':'阵雪',
            '9020':'冰雹',
        }
        return mapper_dict.get(v4_code,'')

    # {'location': 
    #   {'id': 'CN-511112', 'name': '五通桥区', 'country': 'CN', 'path': '五通桥区,乐山市,四川省,中国', 'timezone': 'Asia/Shanghai', 'timezone_offset': '+08:00'},
    # 'data': {'code': '0', 'humidity': '58', 'precip': '0.0', 'pressure': '962.2',
    #  'temperature': '25', 'time': '2020-08-18T16:00:00+08:00', 'visibility': '30.0',
    #  'wind_direction': '南', 'wind_direction_degree': '166',
    #  'wind_scale': '2', 'wind_speed': '7.92', 'text': '晴'}, 
    # 'last_update': '2020-08-18T16:10:25+08:00'}
    def parse_data_meta(self,meta):
        item =  WeatherItem()
        mate_mapper_dict = {
            'province':'prov',
            'city':'city',
            'district':'district',
            'city_short':'city_short',
            'district_short':'district_short',
            'schedule_datetime':'schedule_datetime'
        }
        for k,v in mate_mapper_dict.items():
            item[k] = meta[v]

        item['city_district_short'] = item['city_short']+ item['district_short']

        lnglat = self.location_utils.lnglat(meta['prov'] + meta['city'] + meta['district'])
        item['lng']  = lnglat[0]
        item['lat']  = lnglat[1]
        return item
        

    def parse_data_now(self,item_dict,meta):
        item = self.parse_data_meta(meta)

        mapper_dict ={
            'time':'$.data.time',
            'relative_humidity':'$.data.humidity',
            'pressure':'$.data.pressure',
            'wind_scale':'$.data.wind_scale',
            'wind_direction':'$.data.wind_direction_degree',
            'wind_direction_cn':'$.data.wind_direction',
            'weather':'$.data.text',
            'temperature':'$.data.temperature',
            'rainfall':'$.data.precip',
            'visibility':'$.data.visibility',
            'wind_speed':'$.data.wind_speed',
            'update_time':'$.last_update',
            'code':'$.data.code',
        }

        for k,v in mapper_dict.items():
            item[k] = jsonpath.jsonpath(item_dict,v)[0]

        item['time'] = item['time'].replace('T',' ')[:19]
        item['api_type'] = 'now'
        item['code'] = self.weather_code_v3_v4_mapper(item['code'])
        item['update_time'] = item['update_time'].replace('T',' ')[:19]
        item['orig'] = item_dict

        return item

    #风向	风向角度（左）	风向角度（右）
    # 北	337.51	22.50 
    # 东北	22.51	67.50 
    # 东	67.51	112.50 
    # 东南	112.51	157.50 
    # 南	157.51	202.50 
    # 西南	202.51	247.50 
    # 西	247.51	295.50 
    # 西北	295.51	337.50 
            
    def parse_wind_direction(self,wind_direction):
        wind_direction_index_value = int(float(wind_direction) /22.5)
        wind_direction_index_value =  wind_direction_index_value -1 
        if wind_direction_index_value < 0:
            wind_direction_index_value = 15
        wind_direction_index_value = int(wind_direction_index_value /2)
        wind_direction_index = ['东北','东','东南','南','西南','西','西北','北']
        return wind_direction_index[wind_direction_index_value]

       


    # {'weather_hourly_1h': [
    # {'time_updated': '2020-08-18T06:23:40+08:00', 
    # 'location': {'query': '29.5617:120.0962'}, 
    # 'data': [
    # {'clo': 0, 'gust': 2.76, 'phs': 0, 'pre': 0.0, 'prs_qfe': 999.05, 
    # 'rhu': 73.9, 'ssrd': 0.16, 'tem': 29, 'time': '2020-08-18T08:00:00+08:00', 
    # 'uvb': 0.02, 'vis': 24.13, 'wep': 1000, 'wnd': 161, 'wns': 0.2, 'wns_grd': 0}
    # ]}

    def parse_data_hourly_base(self,item_dict,meta):
        item =  WeatherItem()
        mate_mapper_dict = {
            'province':'province',
            'city':'city',
            'district':'district',
            'city_short':'city_short',
            'district_short':'district_short',
            'schedule_datetime':'schedule_datetime',
            'api_type':'api_type'
        }
        for k,v in mate_mapper_dict.items():
            item[k] = meta[v]

        item['city_district_short'] = item['city_short']+ item['district_short']
        item['update_time']=item_dict['time_updated']

        lnglat = self.location_utils.lnglat(item['province'] + item['city'] + item['district'])
        item['lng']  = lnglat[0]
        item['lat']  = lnglat[1]
        return item

    def parse_data_hourly(self,item,item_dict):
        mapper_dict ={
            'time':'$.time',
            'relative_humidity':'$.rhu',
            'pressure':'$.prs_qfe',
            'wind_scale':'$.wns_grd',
            'wind_direction':'$.wnd',
            'temperature':'$.tem',
            'rainfall':'$.pre',
            'visibility':'$.vis',
            'wind_speed':'$.wns',
            'code':'$.wep',
        }

        try:
            for k,v in mapper_dict.items():
                r = jsonpath.jsonpath(item_dict,v)
                logger.debug(f'k:{k} v:{v} r:{r}')
                item[k] = str(jsonpath.jsonpath(item_dict,v)[0])
        except Exception as e:
            logger.warn(f'item_dict:{item_dict},item:{item} message:{e}')

        item['time'] = item['time'].replace('T',' ')[:19]
        item['wind_direction_cn'] = self.parse_wind_direction(item['wind_direction'])
        item['weather'] = self.weather_code_v4_text(item['code'])
        item['update_time'] = item['update_time'].replace('T',' ')[:19]
        item['orig'] = item_dict

        return item


    #{
	# 	'location': {
	# 		'id': 'WWJ1QCCEC7H5',
	# 		'name': '滨海',
	# 		'country': 'CN',
	# 		'path': '滨海,盐城,江苏,中国',
	# 		'timezone': 'Asia/Shanghai',
	# 		'timezone_offset': '+08:00'
	# 	},
	# 	'alarms': [{
	# 		'title': '滨海县气象台发布高温黄色预警',
	# 		'type': '高温',
	# 		'level': '黄色',
	# 		'status': '',
	# 		'description': '滨海县气象台2020年08月18日11时43分继续发布高温黄色预警信号：预计未来三天我县大部分地区最高气温仍将达35℃以上，请注意防范。',
	# 		'pub_date': '2020-08-18T11:43:44+08:00'
	# 	}]
	# }
    def parse_data_alarm_base(self,item_dict,meta):
        location_str = item_dict['location']['path']
        location_list = location_str.split(',')
        district_short = location_list[0]
        city_short = location_list[1]
        province = location_list[2]
        item = WeatherAlarmItem()
        item['province'] = province
        item['city'] = city_short
        item['city_short'] = city_short
        item['district_short'] = district_short
        item['district'] = district_short
        item['city_district_short'] = item['city_short']+ item['district_short']
        item['schedule_datetime'] = meta['schedule_datetime']
        return item

    def parse_data_alarm(self,item,alarm_dict,meta):
        mapper_dict ={
            'time':'$.pub_date',
            'title':'$.title',
            'type':'$.type',
            'level':'$.level',
            'status':'$.status',
            'description':'$.description',
        }

        for k,v in mapper_dict.items():
            item[k] = jsonpath.jsonpath(alarm_dict,v)[0]
        item['orig'] =  alarm_dict
        return item

    
    def get_now(self,location):
        data_dict = self.requests_get(location,api_type='now')
        logger.debug(data_dict)



        

