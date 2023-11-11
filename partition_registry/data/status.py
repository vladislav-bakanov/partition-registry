from typing import Protocol
import dataclasses as dc

class Status(Protocol): ...

@dc.dataclass(frozen=True)
class SuccessStatus(Status): ...

@dc.dataclass(frozen=True)
class FailedStatus(Status):
    error_message: str

@dc.dataclass(frozen=True)
class SuccededOnAddToQueueStatus(SuccessStatus): ...

@dc.dataclass(frozen=True)
class FailedOnAddToQueueStatus(FailedStatus): ...

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

@dc.dataclass(frozen=True)
class SuccededRegistration(SuccessStatus): ...

@dc.dataclass(frozen=True)
class FailedRegistration(FailedStatus): ...