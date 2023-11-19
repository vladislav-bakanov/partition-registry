import dataclasses as dc
import uuid


@dc.dataclass(frozen=True)
class AccessToken:
    token: str

    @classmethod
    def generate(cls) -> "AccessToken":
        return cls(str(uuid.uuid4()))
