import dataclasses as dc

from typing import Any
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from partition_registry.data.source import SimpleSource
    from partition_registry.data.provider import SimpleProvider
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
    obj: "SimpleSource | SimpleProvider | SimplePartition"
    message: str = dc.field(default="Object already registered...")

@dc.dataclass(frozen=True)
class SuccededRegistration(Success):
    obj: Any
    message: str | None = dc.field(default=None)
    payload: str | None = dc.field(default=None)


@dc.dataclass(frozen=True)
class PartitionReady(Success): ...

@dc.dataclass(frozen=True)
class PartitionNotReady(Success):
    reason: str

@dc.dataclass(frozen=True)
class AccessDenied(Fail): ...
