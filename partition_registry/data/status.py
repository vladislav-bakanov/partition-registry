import dataclasses as dc

from typing import Protocol
from typing import Optional
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from partition_registry.data.partition import UnlockedPartition
    from partition_registry.data.partition import LockedPartition
    from partition_registry.data.source import RegisteredSource
    from partition_registry.data.provider import RegisteredProvider
    from partition_registry.data.partition import RegisteredPartition


@dc.dataclass(frozen=True)
class Status(Protocol):
    ...


@dc.dataclass(frozen=True)
class SuccessStatus(Protocol):
    ...


@dc.dataclass(frozen=True)
class FailedStatus(Status):
    error_message: str


@dc.dataclass(frozen=True)
class SuccededOnAddToQueueStatus(SuccessStatus): ...


@dc.dataclass(frozen=True)
class FailedOnAddToQueueStatus(FailedStatus): ...


@dc.dataclass(frozen=True)
class SuccededPersist(SuccessStatus): ...


@dc.dataclass(frozen=True)
class FailedPersist(FailedStatus): ...


@dc.dataclass(frozen=True)
class SuccededLock(SuccessStatus):
    object: "LockedPartition"
    message: Optional[str] = dc.field(default=None)


@dc.dataclass(frozen=True)
class FailedLock(FailedStatus): ...


@dc.dataclass(frozen=True)
class SuccededUnlock(SuccessStatus):
    object: "UnlockedPartition"
    message: Optional[str] = dc.field(default=None)

@dc.dataclass(frozen=True)
class FailedUnlock(FailedStatus):
    ...


@dc.dataclass(frozen=True)
class SuccededRegistration(SuccessStatus):
    registered_object: "RegisteredSource | RegisteredProvider | RegisteredPartition"
    message: Optional[str] = dc.field(default=None)


@dc.dataclass(frozen=True)
class FailedRegistration(FailedStatus):
    ...


@dc.dataclass(frozen=True)
class NotReadyPartition(FailedStatus): ...


@dc.dataclass(frozen=True)
class ReadyPartition(FailedStatus):
    ready_partition: "UnlockedPartition"


@dc.dataclass(frozen=True)
class ValidationSucceded(SuccessStatus):
    ...
    

@dc.dataclass(frozen=True)
class ValidationFailed(FailedStatus):
    ...
