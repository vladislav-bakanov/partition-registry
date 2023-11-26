import datetime as dt
from partition_registry.data.partition import is_intersected
from partition_registry.data.partition import SimplePartition


def test_intersection_on_not_intersected_objects() -> None:
    p1 = SimplePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 2))
    p2 = SimplePartition(dt.datetime(2000, 1, 3), dt.datetime(2000, 1, 5))

    # Do this to also check, that if we change placement - nothing changes
    result = is_intersected(p1, p2) and is_intersected(p2, p1)

    assert result is False, f"Expected that partitions are not intersected..."


def test_intersection_on_intersected_objects() -> None:
    p1 = SimplePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 3))
    p2 = SimplePartition(dt.datetime(2000, 1, 2), dt.datetime(2000, 1, 5))

    # Do this to also check, that if we change placement - nothing changes
    result = is_intersected(p1, p2) and is_intersected(p2, p1)

    assert result is True, f"Expected that partitions are intersected..."