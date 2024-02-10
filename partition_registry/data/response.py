import dataclasses as dc
from http import HTTPStatus

from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import RegisteredPartition
from partition_registry.data.event import RegisteredPartitionEvent


@dc.dataclass(frozen=True)
class BaseResponse:
    status_code: HTTPStatus


@dc.dataclass(frozen=True)
class SucceededRegistrationResponse(BaseResponse):
    registered_object: RegisteredSource | RegisteredProvider | RegisteredPartition | RegisteredPartitionEvent


@dc.dataclass(frozen=True)
class PartitionReadinessResponse(BaseResponse):
    is_ready: bool
    message: str | None = dc.field(default=None)
