import time

from datetime import datetime

default_datetime_formats =  [
    "%Y%m%d",
    "%Y%m%d%H%M%S",
    "%Y-%m-%d",
    "%Y-%m-%d %a %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    '%m-%d',
    '%m-%d %H:%M',
    '%m-%d %H:%M:%S', 
    '%m-%d %a', 
    '%m-%d %a %H:%M', 
    '%m-%d %a %H:%M:%S', 
]

def convert_cn_datetime(datetime_str):
    datetime_str = str(datetime_str)
    datetime_str = datetime_str.replace('年','-')
    datetime_str = datetime_str.replace('月','-')
    datetime_str = datetime_str.replace('日','')
    datetime_str = datetime_str.replace('周一','Mon')
    datetime_str = datetime_str.replace('周二','Tue')
    datetime_str = datetime_str.replace('周三','Wed')
    datetime_str = datetime_str.replace('周四','Thu')
    datetime_str = datetime_str.replace('周五','Fri')
    datetime_str = datetime_str.replace('周六','Sat')
    datetime_str = datetime_str.replace('周日','Sun')
    return datetime_str

def infer_datetime(datetime_str,datetime_formats=default_datetime_formats,auto_year=True,debug=False):
    datetime_str = convert_cn_datetime(datetime_str)
    for i in datetime_formats:
        try:
            if debug:
                print(f'try datetime_str:{datetime_str} format:{i} ')
            dt = datetime.strptime(datetime_str, i)
            if dt.year == 1900 and auto_year:
                dt = dt.replace(datetime.now().year)
            return dt
        except ValueError as e:
            if debug:
                print(f'datetime_str:{datetime_str} format:{i} error:{e}')
            pass

    return None

def datetime_format(df,format_string='%Y-%m-%d %H:%M:%S',debug=False):
    if isinstance(df,datetime):
        return datetime.strftime(df, format_string)
    elif isinstance(df,str):
        df = infer_datetime(df,debug=debug)
        if df is not None:
            return datetime.strftime(df,format_string)
        return None
    else:
        return None


def ts_to_str(ts,ts_format = "%Y-%m-%d %H:%M:%S"):
    return time.strftime(ts_format, time.localtime(ts))

def str_to_ts(ts,ts_format = "%Y-%m-%d %H:%M:%S.%f"):
    return time.mktime(time.strptime(ts, ts_format))

def str_to_datetime(datetime_str,ts_format = "%Y-%m-%d %H:%M:%S"):
    return infer_datetime(datetime_str)

def ts_to_datetime(ts,ts_format="%Y-%m-%d %H:%M:%S"):
    return datetime.strptime(time.strftime(ts_format, time.localtime(ts)),ts_format)

def ts_to_date(ts,ts_format='%Y-%m-%d'):
    return ts_to_datetime(ts,ts_format)

def date_to_str(dt,ts_format='%Y-%m-%d'):
    return dt.strftime( ts_format )  

def datetime_to_str(dt,ts_format="%Y-%m-%d %H:%M:%S"):
    return dt.strftime( ts_format )  

