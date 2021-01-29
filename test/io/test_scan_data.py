from python_common.utils.datetime_utils import  str_to_datetime
from python_common.io.scan_datafiles import ScanDataFiles,true_filter,false_filter

from datetime import datetime,timedelta



def level1_dir_filter(filepath,filename,last_dir=None):
    try:
        dir_date = str_to_datetime(filename)
        if  dir_date.date() >=  (datetime.now() - timedelta(hours=5)).date():
            return True
    except Exception as e:
        print(e)

    return False


def level2_dir_filter(filepath,filename,dir_list):
    try:
        dt_str = f'{dir_list[0]} {filename}'
        dt =  datetime.strptime(dt_str, "%Y-%m-%d %H")
        if dt >=  (datetime.now() -  timedelta(hours=5)):
            return True
        else:
            return False
    except Exception as e:
        print(e)

    return False


def level2_file_filter(filepath,filename,dir_list):
    try：
        if filename.startswith('user_behavior_data') and not filename.endswith('.in-progress')  and not filename.endswith('.pending')：
            return True
    except Exception as e:
        print(e)

    return False    
    


dir_filters = [
    level1_dir_filter,
    level2_dir_filter,
    true_filter
]


file_filters = level2_file_filter


s = ScanDataFiles('test','/ai/suanfa/user_behavior_data_min',file_filters=false_filter,dir_filters=dir_filters)
s.scan()
s.process_files()