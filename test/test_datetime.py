from datetime import datetime,timedelta
from python_common.utils.datetime_utils import infer_datetime
from python_common.utils.datetime_utils import datetime_format
# for i  in range(7):
#     print(datetime_format(datetime.now() + timedelta(days=i)))

dt = infer_datetime('05月20日')
print(dt)
dt = infer_datetime('05月20日 周三 14:00')
print(dt)


print(datetime.now().minute)