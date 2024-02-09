import dataclasses as dc

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from partition_registry.data.source import Source
    from partition_registry.data.source import SimpleSource
    from partition_registry.data.provider import Provider
    from partition_registry.data.provider import SimpleProvider
    from partition_registry.data.partition import Partition
    from partition_registry.data.partition import SimplePartition


@dc.dataclass(frozen=True)
class Status: ...


@dc.dataclass(frozen=True)
class Success(Status): ...


@dc.dataclass(frozen=True)
class Fail(Status):
    message: str

@dc.dataclass(frozen=True)
class SuccededPersist(Success): ...

@dc.dataclass(frozen=True)
class FailedPersist(Fail): ...

@dc.dataclass(frozen=True)
class ValidationSucceded(Success): ...

@dc.dataclass(frozen=True)
class ValidationFailed(Fail): ...

@dc.dataclass(frozen=True)
class FailedRegistration(Fail): ...

@dc.dataclass(frozen=True)
class LookupFailed(Fail): ...

@dc.dataclass(frozen=True)
class AlreadyRegistered(Status):
    obj: SimpleSource | SimpleProvider | SimplePartition
    message: str | None = dc.field(default=None)

@dc.dataclass(frozen=True)
class SuccededRegistration(Success):
    obj: Source | Provider | Partition
    message: str | None
    payload: str | None


@dc.dataclass(frozen=True)
class PartitionReady(Success): ...

@dc.dataclass(frozen=True)
class PartitionNotReady(Success):
    reason: str | None
