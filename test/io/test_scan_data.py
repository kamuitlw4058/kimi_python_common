from python_common.utils.datetime_utils import  str_to_datetime
from python_common.io.scan_datafiles import ScanDataFiles

from datetime import datetime,timedelta



def level1_dir_filter(filepath,filename):
    try:
        dir_date = str_to_datetime(filename)
        if  dir_date.date() >=  (datetime.now() - timedelta(hours=5)).date():
            return True
    except Exception as e:
        print(e)

    return False


def level2_dir_filter(filepath,filename):
    try:
        hour = int(filename)
        return True
    except Exception as e:
        print(e)

    return False



dir_filters = [
    level1_dir_filter,
    level2_dir_filter,
    true_filter

]


s = ScanDataFiles('test','/ai/suanfa/user_behavior_data_min',file_filters=false_filter,dir_filters=dir_filters)
s.scan()
s.process_files()