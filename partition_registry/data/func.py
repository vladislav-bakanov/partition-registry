import datetime as dt

def safe_parse_datetime(obj: bytes) -> dt.datetime | None:
    fmt = '%Y-%m-%d %H:%M:%S.%f%z'
    decoded_object = obj.decode('utf-8')
    match decoded_object:
        case str() as string_decoded_obj:
            try:
                datetime_obj = dt.datetime.strptime(string_decoded_obj, fmt)
            except ValueError as e:
                return None

            return datetime_obj


def generate_unixtime(obj: dt.datetime) -> float:
    return dt.datetime.timestamp(obj) * 1000