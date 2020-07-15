import logging

from zmc_common.location.gaode import lnglat
from zmc_common.location.gaode import CommonLocationOverLimitException
from zmc_common.location.gaode import logger as test_module_logger

if __name__ == "__main__":
    #test_module_logger.setLevel(logging.DEBUG)
    try:
        print(lnglat('上海市'))
    except CommonLocationOverLimitException as e:
        print(e)

 