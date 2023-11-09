from partition_registry.data.partition import UnlockedPartition
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
message = (
        "You are trying to get root page of Partition Registry Service. "
        "Please, visit our documentation page (YOUR_URL/redocs) to get to "
        "know with complete functional"
    )
print(message)