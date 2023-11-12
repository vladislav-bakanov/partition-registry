import dataclasses as dc
from typing import Protocol
from typing import Any

class BaseResponse(Protocol):
    status_code: int
    message: str
    data: dict[str, Any]


@dc.dataclass(frozen=True)
class APIResponse(BaseResponse):
    status_code: int
    message: str
    data: dict[str, Any]