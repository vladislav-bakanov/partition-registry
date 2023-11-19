import datetime as dt
import hypothesis.strategies as st
from hypothesis import assume

from partition_registry.data.partition import SimplePartition

def build_simple_partition(start: dt.datetime, end: dt.datetime) -> SimplePartition:
    assume(end > start)
    return SimplePartition(start, end)

arbitrary_simple_partition = st.builds(build_simple_partition, st.datetimes(), st.datetimes())
