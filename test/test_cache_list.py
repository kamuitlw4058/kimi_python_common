import logging

from zmc_common.utils.cache_dict_list import CachedDictList
from zmc_common.utils.cache_dict_list import logger as test_module_logger

if __name__ == "__main__":
    test_module_logger.setLevel(logging.DEBUG)
    cdict = CachedDictList('test_key')
    d =  cdict.get('test1')
    print(d)
    cdict.append_cache("test1",{'test':123})

    d =  cdict.get('test1')
    print(d)

    
