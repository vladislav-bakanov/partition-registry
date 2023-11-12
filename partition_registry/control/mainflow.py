import datetime as dt
from typing import Any
from http import HTTPStatus

from fastapi import FastAPI

from partition_registry.data.partition import UnknownPartition
from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition

from partition_registry.data.source import SimpleSource

from partition_registry.actor.queue import MainQueue
from partition_registry.data.provider import Provider

from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import EXIST
from partition_registry.data.status import NOT_EXIST
from partition_registry.data.response import APIResponse

from partition_registry.actor.registry.source import SourceRegistry
from partition_registry.actor.registry.provider import ProviderRegistry
from partition_registry.actor.registry.partition.orm import PartitionRegistryORM
from partition_registry.actor.registry.source import SourceRegistryORM
from partition_registry.actor.registry.provider import ProviderRegistryORM

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

app = FastAPI()

engine = create_engine('sqlite:///mydatabase.db', echo=True)
session = Session(engine)

source_registry = SourceRegistry(SourceRegistryORM)
provider_registry = ProviderRegistry(ProviderRegistryORM)
queue = MainQueue(100, PartitionRegistryORM, session)

@app.get("/")
def read_root() -> dict[str, str]:
    message = (
        "You are trying to get root page of Partition Registry Service. "
        f"Please, visit documentation page by address YOUR_URL/redoc to get to "
        "know with complete functional"
    )
    return {"status_code": HTTPStatus.OK, "message": message}


@app.post("/sources/{source}/register")
def register_source(source: str) -> dict[str, Any]:
    source = SimpleSource(source)
    status = source_registry.register(source)
    match status:
        case FailedRegistration():
            return {
                "status_code": HTTPStatus.CONFLICT,
                "message": status.error_message
            }
        case SuccededRegistration():
            return {
                "status_code": HTTPStatus.OK,
                "message": "Successfully registered",
                "source": source_registry.get_registered_source(source)
            }


@app.get("/sources/{source}")
def get_source_info(source: str):
    source = SimpleSource(source)
    match source_registry.is_registered(source):
        case NOT_EXIST():
            return {
                "status_code": HTTPStatus.BAD_REQUEST,
                "message": f"Source \"{source}\" doesn't exist. Please, register source to proceed..."
            }
        case EXIST():
            return {
                "status_code": HTTPStatus.OK,
                "message": "",
                "source": source_registry.get_registered_source(source)
            }
        case s:
            raise ValueError(f"Expected EXIST | NOT_EXIST status, got {s}")


@app.post("/sources/{source}/{provider}/register")
def register_provider(
    source: str,
    provider: str
) -> dict[str, Any]:
    source = SimpleSource(source)
    status = source_registry.get_registered_source(source)
    match status:
        case NOT_EXIST():
            return {
                "status_code": HTTPStatus.CONFLICT,
                "message": f"Can't register provider, because source {source} hasn't been registered..."
            }
        case registered_source:
            provider = Provider(provider, registered_source)
            return {
                "status_code": HTTPStatus.OK,
                "message": "Successfully registered",
                "provider": provider
            }


@app.post("/sources/{source}/{provider}/lock")
def lock_partition(
    source: str,
    provider: str,
    start: dt.datetime,
    end: dt.datetime
) -> dict[str, Any]:
    source = SimpleSource(source)
    partition = UnknownPartition(start, end)
    msg = f"You locked the partition \"{partition}\" by the source \"{str(source)}\""
    return {
        "status_code": HTTPStatus.OK,
        "message": msg
    }


@app.post("/sources/{source}/{provider}/unlock")
def unlock_partition(
    source: str,
    provider: str,
    start: dt.datetime,
    end: dt.datetime,
) -> dict[str, Any]:
    provider = Provider(provider, source)
    partition = LockedPartition(start, end, provider)
    msg = f"You unlocked the partition {partition}"
    return {
        "status_code": HTTPStatus.OK,
        "message": msg
    }


# http://127.0.0.1:8000/registry/bigquery/sources/prodcloudna-de-production.registry.partition_registry/partitions/readiness/?partition_start=2020-01-01&partition_end=2020-02-01
@app.get("/sources/{source}/is.ready")
def is_source_ready(
    source: str,
    start: dt.datetime,
    end: dt.datetime,
) -> dict[str, Any]:
    source = Source(source)
    partition = UnknownPartition(source, start, end)
    return {
        "status_code": HTTPStatus.OK,
        "source": source,
        "partition": str(partition),
        "is_ready": True,
        "blocked_by": "NAME_OF_PROVIDER"  # TODO: ...
    }
