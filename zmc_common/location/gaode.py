import requests
import random

key_list  =[ 
    '158aece6394e2a8de2e432bdef7d9427', # 飞哥
    'a1e21b86523c040bec27410f8103de8c', # 王岑晗
    '72107dc0ff737e02cd2d746727d401d7', # 范静
    '000e0f6badb3dbc853b2cbb0ec16ad08', # 范国金
    'fba16c9c0b911b89c7390b867b9a8026', # 宋佳敏
    ]


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

def _get_key():
    return  key_list[random.randint(0,len(key_list) -1)]

def location(address):
    key = _get_key()
    url = f'https://restapi.amap.com/v3/geocode/geo?key={key}&address={address}'
    r = requests.get(url)
    j = r.json()
    return j

def lnglat(address):
    key = _get_key()
    try:
        url = f'https://restapi.amap.com/v3/geocode/geo?key={key}&address={address}'
        r = requests.get(url)
        j = r.json()
        location  = ''
        location = [float(i)  for i in  str(j['geocodes'][0]['location']).split(","))
    except Exception as e:
        location =None
    return location 

if __name__ == "__main__":
    print(location('北京市昌平区马横路167号靠近横桥社区卫生服务站'))