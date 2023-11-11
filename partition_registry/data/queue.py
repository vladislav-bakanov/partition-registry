from typing import Protocol
from typing import Set
from typing import List
from typing import Set
from typing import Dict
from typing import Tuple
from typing import Iterable

from collections import defaultdict
from functools import cached_property

import datetime as dt
import os

from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition
from partition_registry.data.partition import UnknownPartition

from partition_registry.data.provider import Provider

from partition_registry.data.status import SuccededPersist
from partition_registry.data.status import FailedPersist

from partition_registry.data.status import SuccededOnAddToQueueStatus
from partition_registry.data.status import FailedOnAddToQueueStatus

from partition_registry.data.status import SuccededPurification
from partition_registry.data.status import FailedPurification


class Queue(Protocol):
    def put(self, partition: LockedPartition | UnlockedPartition) -> SuccededOnAddToQueueStatus | FailedOnAddToQueueStatus: ...
    # def pull(self) -> None: ...
    def purify(self) -> SuccededPurification | FailedPurification: ...
    def persist(self) -> SuccededPersist | FailedPersist: ...
    # def __next__(self) -> self:
    #     return self
    # def __iter__(self) -> Iterable[str]: ...
    
    @property
    def size(self) -> int: ...
