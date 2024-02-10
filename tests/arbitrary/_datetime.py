"All datetime objects, used for property testing"

from hypothesis import strategies as st

arbitrary_datetime_wo_timezone = st.datetimes()
arbitrary_datetime_with_timezone = st.datetimes(timezones=st.timezones())

arbitrary_datetime = st.one_of([
    arbitrary_datetime_wo_timezone,
    arbitrary_datetime_with_timezone,
])