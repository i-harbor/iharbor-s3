from datetime import datetime
from pytz import utc


GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'


def datetime_from_gmt(value):
    """
    :param value: gmt格式时间字符串
    :return:
        datetime() or None
    """
    try:
        t = datetime.strptime(value, GMT_FORMAT)
        return t.replace(tzinfo=utc)
    except Exception as e:
        return None

