from datetime import datetime

def convert_datetime_to_date(datetime_format='%Y-%m-%d %H:%M:%S'):
    def func(value):
        return datetime.strptime(str(value),datetime_format).date()
    return func