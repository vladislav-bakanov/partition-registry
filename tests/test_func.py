import datetime as dt

from hypothesis import given

from tests.arbitrary._datetime import arbitrary_datetime_with_timezone
from tests.arbitrary._datetime import arbitrary_datetime

from partition_registry.data.func import localize


@given(obj=arbitrary_datetime)
def test_localization_object_wo_tz(obj: dt.datetime) -> None:
    d = localize(obj)
    assert d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None, \
        "Expected object with tz"
