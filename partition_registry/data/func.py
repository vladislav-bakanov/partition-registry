import datetime as dt
from dateutil import tz

def localize(obj: dt.datetime) -> dt.datetime:
    """Transfer naive datetime object to timezone aware UTC object"""
    if obj.tzinfo is None or obj.tzinfo.utcoffset(obj) is None:
        return obj.replace(tzinfo=tz.UTC)
    return obj
