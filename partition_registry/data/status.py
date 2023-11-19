from typing import Protocol
from typing import Optional

import dataclasses as dc
from typing import Any

class Status(Protocol):
    ...


@dc.dataclass(frozen=True)
class SuccessStatus(Status):
    ...


@dc.dataclass(frozen=True)
class FailedStatus(Status):
    error_message: str


@dc.dataclass(frozen=True)
class SuccededOnAddToQueueStatus(SuccessStatus):
    ...


@dc.dataclass(frozen=True)
class FailedOnAddToQueueStatus(FailedStatus):
    ...


@dc.dataclass(frozen=True)
class SuccededPersist(SuccessStatus):
    ...


@dc.dataclass(frozen=True)
class FailedPersist(FailedStatus):
    ...


@dc.dataclass(frozen=True)
class SuccededPurification(SuccessStatus):
    ...


@dc.dataclass(frozen=True)
class FailedPurification(FailedStatus):
    ...


@dc.dataclass(frozen=True)
class SuccededLock(SuccessStatus):
    locked_object: Any
    message: Optional[str] = dc.field(default=None)


@dc.dataclass(frozen=True)
class FailedLock(FailedStatus):
    ...


@dc.dataclass(frozen=True)
class SuccededUnlock(SuccessStatus):
    ...


@dc.dataclass(frozen=True)
class FailedUnlock(FailedStatus):
    ...


@dc.dataclass(frozen=True)
class SuccededRegistration(SuccessStatus):
    registered_object: Any
    message: Optional[str] = dc.field(default=None)


@dc.dataclass(frozen=True)
class FailedRegistration(FailedStatus):
    ...
