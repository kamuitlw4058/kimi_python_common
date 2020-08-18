
import urllib.parse
import time
from collections import OrderedDict

def dict2url(input_dict,
            sort_by_key=False,
            urlencode=False,
            ts_key='ts',override_ts=True,
            ttl_key='ttl',ttl_default=300,override_ttl=True):
    if ts_key is not None and ( (override_ts) or (not override_ts and input_dict.get(ts_key,None) is None)):
        input_dict[ts_key]=int(time.time())
    
    if ttl_key is not None and ( (override_ttl) or (not override_ttl and input_dict.get(ttl_key,None) is None)):
        input_dict[ttl_key]=ttl_default

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