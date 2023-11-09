from partition_registry_v2.data.partition import UnlockedPartition
import datetime as dt


t = UnlockedPartition(
    dt.datetime(2000, 1, 1),
    dt.datetime(2000, 1, 2),
    # created_at=dt.datetime.now(),
    # locked_at=dt.datetime.now(),
    # unlocked_at=dt.datetime.now(),
)

# print(str(t))

from collections import defaultdict

t = defaultdict(set)
print(t)