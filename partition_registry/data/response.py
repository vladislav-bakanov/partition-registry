from typing import Any

class BaseResponse:
    success: bool
    error_message: str
    data: Any