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

def infer_datetime(datetime_str,datetime_formats=default_datetime_formats,auto_year=True):
    datetime_str = convert_cn_datetime(datetime_str)
    for i in datetime_formats:
        try:
            dt = datetime.strptime(datetime_str, i)
            if dt.year == 1900 and auto_year:
                dt = dt.replace(datetime.now().year)
            return dt
        except ValueError as e:
            print(e)
            pass

    return None

def datetime_format(df,format='%Y-%m-%d %a %H:%M:%S'):
    return datetime.strftime(df, format)
