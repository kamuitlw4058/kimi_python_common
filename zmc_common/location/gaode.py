import requests
import random

key_list  =[ 
    '158aece6394e2a8de2e432bdef7d9427', # 飞哥
    'a1e21b86523c040bec27410f8103de8c', # 王岑晗
    '72107dc0ff737e02cd2d746727d401d7', # 范静
    '000e0f6badb3dbc853b2cbb0ec16ad08', # 范国金
    'fba16c9c0b911b89c7390b867b9a8026', # 宋佳敏
    'f912108a689c0cebf24e7fc064f0bc95', # 唐宁
    ]

key_dic = None



class CommonLocationOverLimitException(Exception):
    def __init__(self,msg):
        self.msg = msg
    def __str__(self):
        return self.msg

def get_example():
    return {'status': '1', 
    'info': 'OK', 
    'infocode': '10000',
     'count': '1', 
     'geocodes': [
            {'formatted_address':
            '北京市昌平区横桥社区卫生服务站',
            'country': '中国', 
            'province': '北京市', 
            'citycode': '010',
            'city': '北京市',
            'district': '昌平区',
            'township': [],
            'neighborhood': {'name': [], 'type': []}, 
            'building': {'name': [], 'type': []}, 
            'adcode': '110114', 
            'street': [], 
            'number': [], 
            'location': '116.192951,40.172759', 
            'level': '兴趣点'}
        ]
    }

def get_error_example_over_limit():
    return {'status': '0', 'info': 'DAILY_QUERY_OVER_LIMIT', 'infocode': '10003'}


def _get_key():
    global key_dic
    global key_list

    if key_dic is None:
        key_dic = {}
        for i in key_list:
            key_dic[i] = 1

    return  list(key_dic)[random.randint(0,len(key_dic) -1)]

def _pop_key(key):
    global key_dic
    del key_dic[key]

def location(address,input_key = None):
    global key_dic
    if input_key is None:
        key = _get_key()
    else:
        key = input_key

    url = f'https://restapi.amap.com/v3/geocode/geo?key={key}&address={address}'
    r = requests.get(url)
    j = r.json()
    if j['info'] == 'DAILY_QUERY_OVER_LIMIT' and input_key is None:
        _pop_key(key)
        if len(key_dic) == 0:
            raise CommonLocationOverLimitException("DAILY_QUERY_OVER_LIMIT")
        else:
            return None

    elif j['info'] == 'DAILY_QUERY_OVER_LIMIT' and input_key is not None:
        raise CommonLocationOverLimitException("DAILY_QUERY_OVER_LIMIT")
    return j

def lnglat(address,input_key=None):
    try:
        j  = location(address,input_key=input_key)
        ret  = ''
        ret = [float(i)  for i in  str(j['geocodes'][0]['location']).split(",")]
    except KeyError as e:
        ret =None
    except IndexError as e:
        ret =None
    except TypeError as e:
        ret =None
    
    return ret 

if __name__ == "__main__":
    try:
        print(lnglat('北京市'))
    except CommonLocationOverLimitException as e:
        print(e)
    print(key_dic)

 