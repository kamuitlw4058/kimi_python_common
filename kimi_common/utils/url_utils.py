
import urllib.parse
import time
from collections import OrderedDict

default_auto_key=[
    ('ts',False,lambda : int(time.time())),
    ('ttl',False,lambda : 300),
]

def dict2url(input_dict,
            sort_by_key=False,
            urlencode=False,
            auto_key = default_auto_key
            ):
    for key,override,defaut_func in auto_key:
        if ((override) or (not override and input_dict.get(key,None) is None)):
            input_dict[key]=defaut_func()
        
    if sort_by_key:
        input_list= sorted(input_dict.items(),key=lambda x:x[0])
        input_dict = OrderedDict()
        for i in input_list:
            input_dict[i[0]] = i[1]
    if urlencode:
        return urllib.parse.urlencode(input_dict)
    else:
        ret_list = []
        for k,v in input_dict.items():
            ret_list.append(f'{k}={v}')

        return '&'.join(ret_list)