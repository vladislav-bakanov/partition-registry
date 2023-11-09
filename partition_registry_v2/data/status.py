from typing import Protocol
import dataclasses as dc

@dc.dataclass(frozen=True)
class Status(Protocol): ...

@dc.dataclass(frozen=True)
class SuccessStatus(Status): ...

@dc.dataclass(frozen=True)
class FailedStatus(Status):
    error_message: str

@dc.dataclass(frozen=True)
class SuccededOnAddToQueueStatus(SuccessStatus): ...

@dc.dataclass(frozen=True)
class FailedOnAddToQueueStatus(FailedStatus):
    pass

@dc.dataclass(frozen=True)
class SuccededPersist(Status): ...

@dc.dataclass(frozen=True)
class FailedPersist(FailedStatus): ...

@dc.dataclass(frozen=True)
class SuccededPurification(Status): ...

@dc.dataclass(frozen=True)
class FailedPurification(FailedStatus): ...

@dc.dataclass(frozen=True)
class SuccededLock(Status): ...

@dc.dataclass(frozen=True)
class FailedLock(FailedStatus): ...

@dc.dataclass(frozen=True)
class SuccededUnlock(Status): ...

@dc.dataclass(frozen=True)
class FailedUnlock(FailedStatus): ...
