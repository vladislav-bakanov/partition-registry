import dataclasses as dc

from typing import Protocol

from http import HTTPStatus

from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import RegisteredPartition

from partition_registry.data.source import SimpleSource
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition


class BaseResponse(Protocol):
    status_code: HTTPStatus


@dc.dataclass(frozen=True)
class RegistrationResponse(BaseResponse):
    status_code: HTTPStatus
    source: RegisteredSource | RegisteredProvider | RegisteredPartition


@dc.dataclass(frozen=True)
class PartitionLockResponse(BaseResponse):
    status_code: HTTPStatus
    message: str
    source: SimpleSource
    provider: SimpleProvider
    partition: LockedPartition


@dc.dataclass(frozen=True)
class PartitionUnlockResponse(BaseResponse):
    status_code: HTTPStatus
    message: str
    source: SimpleSource
    provider: SimpleProvider
    partition: UnlockedPartition


@dc.dataclass(frozen=True)
class PartitionReadyResponse(BaseResponse):
    status_code: HTTPStatus
    message: str
    source: SimpleSource
    is_ready: bool = dc.field(init=False, default=True)


@dc.dataclass(frozen=True)
class PartitionNotReadyResponse(BaseResponse):
    status_code: HTTPStatus
    message: str
    source: SimpleSource
    is_ready: bool = dc.field(init=False, default=False)
