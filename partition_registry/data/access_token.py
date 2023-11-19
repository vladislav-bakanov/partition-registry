import dataclasses as dc
import hashlib
import uuid
from functools import cached_property


@dc.dataclass(frozen=True)
class AccessToken:
    token: str
    
    @classmethod
    def generate(cls) -> "AccessToken":
        return cls(str(uuid.uuid4()))