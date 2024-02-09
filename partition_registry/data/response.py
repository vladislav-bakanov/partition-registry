import dataclasses as dc
from http import HTTPStatus

from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import RegisteredPartition
from partition_registry.data.event import RegisteredPartitionEvent


dc.dataclass(frozen=True)
class BaseResponse:
    status_code: HTTPStatus


class RegistrationResponse(BaseResponse):
    object: RegisteredSource | RegisteredProvider | RegisteredPartition | RegisteredPartitionEvent


class PartitionReadinessResponse(BaseResponse):
    is_ready: bool
    message: str | None = dc.field(default=None)
