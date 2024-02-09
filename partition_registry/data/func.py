import datetime as dt
from dateutil import tz

def localize(d: dt.datetime) -> dt.datetime:
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=tz.UTC)
    return d
