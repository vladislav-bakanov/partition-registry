import dataclasses as dc

from typing import Protocol
from typing import Optional
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from partition_registry.data.partition import UnlockedPartition
    from partition_registry.data.event import RegisteredPartitionEvent
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
class SuccededPersist(SuccessStatus): ...


@dc.dataclass(frozen=True)
class FailedPersist(FailedStatus): ...


@dc.dataclass(frozen=True)
class SuccededRegistration(SuccessStatus):
    registered_object: "RegisteredSource | RegisteredProvider | RegisteredPartition | RegisteredPartitionEvent"
    message: Optional[str] = dc.field(default=None)


@dc.dataclass(frozen=True)
class FailedRegistration(FailedStatus): ...


@dc.dataclass(frozen=True)
class PartitionReady(SuccessStatus): ...


@dc.dataclass(frozen=True)
class PartitionNotReady(Status):
    reason: str | None


@dc.dataclass(frozen=True)
class ValidationSucceded(SuccessStatus):
    ...
    

@dc.dataclass(frozen=True)
class ValidationFailed(FailedStatus):
    ...
