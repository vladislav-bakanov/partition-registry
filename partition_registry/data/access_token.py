import dataclasses as dc
import uuid


@dc.dataclass(frozen=True)
class AccessToken:
    token: str

    @classmethod
    def generate(cls) -> "AccessToken":
        """Generates random access token"""
        return cls(str(uuid.uuid4()))

    def __str__(self) -> str:
        # TODO: add sha256 representation
        ...
    
    def __repr__(self) -> str:
        # TODO: add sha256 representation
        ...