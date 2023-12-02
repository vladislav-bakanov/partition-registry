"All datetime objects, used for property testing"

from hypothesis import strategies as st

arbitrary_datetime = st.datetimes()
