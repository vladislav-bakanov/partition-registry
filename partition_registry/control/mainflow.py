import datetime as dt
from typing import Any
from http import HTTPStatus

from fastapi import FastAPI
from fastapi import HTTPException

from partition_registry.orm import PartitionRegistryORM

from partition_registry.data.partition import SimplePartition
from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition
from partition_registry.data.access_token import AccessToken
from partition_registry.data.source import SimpleSource
from partition_registry.data.source import RegisteredSource
from partition_registry.actor.main_queue import MainQueue
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedLock
from partition_registry.data.status import SuccededLock
from partition_registry.data.response import SourceRegistrationResponse
from partition_registry.data.response import PartitionLockResponse

from partition_registry import action

from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry
from partition_registry.actor.registry.partition_registry import PartitionRegistry
from partition_registry.orm import SourceRegistryORM
from partition_registry.orm import ProviderRegistryORM

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

app = FastAPI()

# engine = create_engine('sqlite:///mydatabase.db', echo=True)
# session = Session(engine)

source_registry = SourceRegistry(SourceRegistryORM)
provider_registry = ProviderRegistry(ProviderRegistryORM)
partition_registry = PartitionRegistry(PartitionRegistryORM)
# queue = MainQueue(100, PartitionRegistryORM, session)

@app.get("/")
def read_root() -> dict[str, str | int]:
    message = (
        "You are trying to get root page of Partition Registry Service. "
        f"Please, visit documentation page by address YOUR_URL/redoc to get to "
        "know with complete functional"
    )
    return {"status_code": HTTPStatus.OK, "message": message}


@app.post("/sources/register")
def register_source(source_name: str) -> dict[str, Any]:
    response = action.register_source(source_name, source_registry)
    match response:
        case FailedRegistration():
            return HTTPException(HTTPStatus.CONFLICT, response.error_message).__dict__
        case SuccededRegistration():
            return SourceRegistrationResponse(HTTPStatus.OK, response.registered_object).__dict__


@app.post("/sources/{source_name}/{provider_name}/lock")
def lock_partition(
    source_name: str,
    provider_name: str,
    access_token: str,
    partition_start: dt.datetime,
    partition_end: dt.datetime
) -> dict[str, Any]:
    
    response = action.lock_partition(
        source_name,
        provider_name,
        access_token,
        partition_start,
        partition_end,
        partition_registry,
        provider_registry,
        source_registry
    )
    
    match response:
        case FailedLock():
            return HTTPException(HTTPStatus.CONFLICT, response.error_message)
        case SuccededLock():
            return PartitionLockResponse(
                status_code=HTTPStatus.OK,
                message="Succeded lock...",
                source=SimpleSource(source_name),
                provider=SimpleProvider(provider_name),
                partition=response.locked_object
            )



# @app.post("/sources/{source}/{provider}/unlock")
# def unlock_partition(
#     source: str,
#     provider: str,
#     start: dt.datetime,
#     end: dt.datetime,
# ) -> dict[str, Any]:
#     provider = Provider(provider, source)
#     partition = LockedPartition(start, end, provider)
#     msg = f"You unlocked the partition {partition}"
#     return {
#         "status_code": HTTPStatus.OK,
#         "message": msg
#     }


# # http://127.0.0.1:8000/registry/bigquery/sources/prodcloudna-de-production.registry.partition_registry/partitions/readiness/?partition_start=2020-01-01&partition_end=2020-02-01
# @app.get("/sources/{source}/is.ready")
# def is_source_ready(
#     source: str,
#     start: dt.datetime,
#     end: dt.datetime,
# ) -> dict[str, Any]:
#     source = Source(source)
#     partition = UnknownPartition(source, start, end)
#     return {
#         "status_code": HTTPStatus.OK,
#         "source": source,
#         "partition": str(partition),
#         "is_ready": True,
#         "blocked_by": "NAME_OF_PROVIDER"  # TODO: ...
#     }
