import datetime
import pytz

def datetimeField(current_datetime:datetime.datetime):
    if current_datetime.hour < 16:
        res = (current_datetime - datetime.timedelta(1))\
            .strftime('%Y%m%d %H%M')
        res = res[:-4] + '1600'
    else:
        res = (current_datetime - datetime.timedelta(1))\
            .strftime('%Y%m%d %H%M')
        res = res[:-4] + '1600'
    return res