import datetime as dt
from typing import Any
from http import HTTPStatus
from contextlib import closing

from fastapi import FastAPI
from fastapi import HTTPException

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session

from partition_registry import actions

from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry
from partition_registry.actor.registry import PartitionRegistry

from partition_registry.data.source import SimpleSource
from partition_registry.data.provider import SimpleProvider

from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedLock
from partition_registry.data.status import FailedUnlock
from partition_registry.data.status import SuccededLock
from partition_registry.data.status import SuccededUnlock

from partition_registry.data.response import RegistrationResponse
from partition_registry.data.response import PartitionLockResponse
from partition_registry.data.response import PartitionUnlockResponse
from partition_registry.data.response import PartitionReadyResponse
from partition_registry.data.response import PartitionNotReadyResponse


app = FastAPI()

engine = create_engine('postgresql+psycopg2://postgres:changeme@localhost/partition_registry', echo=True)

session = Session(engine)
source_registry = SourceRegistry(session)
provider_registry = ProviderRegistry(session)
partition_registry = PartitionRegistry(session)


@app.get("/")
def read_root() -> dict[str, str | int]:
    message = (
        "You are trying to get root page of Partition Registry Service. "
        "Please, visit documentation page by address YOUR_URL/redoc to get to "
        "know with complete functional"
    )
    return {"status_code": HTTPStatus.OK, "message": message}


@app.post("/sources/register")
def register_source(source_name: str, owner: str) -> dict[str, Any]:
    response = actions.register_source(source_name, owner, source_registry)
    match response:
        case FailedRegistration():
            return HTTPException(HTTPStatus.CONFLICT, response.error_message).__dict__
        case SuccededRegistration():
            return RegistrationResponse(HTTPStatus.OK, response.registered_object).__dict__


@app.post("/providers/register")
def register_provider(provider_name: str, access_token: str) -> dict[str, Any]:
    response = actions.register_provider(provider_name, access_token, provider_registry)
    match response:
        case FailedRegistration():
            return HTTPException(HTTPStatus.CONFLICT, response.error_message).__dict__
        case SuccededRegistration():
            return RegistrationResponse(HTTPStatus.OK, response.registered_object).__dict__


@app.post("/partitions/register")
def register_partition(
    start: dt.datetime,
    end: dt.datetime,
    source_name: str,
    provider_name: str
) -> dict[str, Any]:
    response = actions.register_partition(
        start,
        end,
        partition_registry,
        source_name,
        source_registry,
        provider_name,
        provider_registry
    )
    match response:
        case FailedRegistration():
            return HTTPException(HTTPStatus.CONFLICT, response.error_message).__dict__
        case SuccededRegistration():
            return RegistrationResponse(HTTPStatus.OK, response.registered_object).__dict__


@app.post("/partitions/lock")
def lock_partition(
    start: dt.datetime,
    end: dt.datetime,
    source_name: str,
    provider_name: str
) -> dict[str, Any]:
    response = actions.lock_partition(
        start,
        end,
        partition_registry,
        source_name,
        source_registry,
        provider_name,
        provider_registry
    )
    match response:
        case FailedRegistration():
            return HTTPException(HTTPStatus.CONFLICT, response.error_message).__dict__
        case SuccededRegistration():
            return RegistrationResponse(HTTPStatus.OK, response.registered_object).__dict__


# @app.post("/sources/{source_name}/{provider_name}/lock")
# def lock_partition(
#     source_name: str,
#     provider_name: str,
#     access_token: str,
#     partition_start: dt.datetime,
#     partition_end: dt.datetime
# ) -> dict[str, Any]:

#     response = actions.lock_partition(
#         source_name,
#         provider_name,
#         access_token,
#         partition_start,
#         partition_end,
#         partition_registry,
#         provider_registry,
#         source_registry
#     )

#     match response:
#         case FailedLock():
#             return HTTPException(HTTPStatus.CONFLICT, response.error_message).__dict__
#         case SuccededLock():
#             return PartitionLockResponse(
#                 status_code=HTTPStatus.OK,
#                 message="Succeded lock...",
#                 source=SimpleSource(source_name),
#                 provider=SimpleProvider(provider_name),
#                 partition=response.locked_object
#             ).__dict__


# @app.post("/sources/{source_name}/{provider_name}/unlock")
# def unlock_partition(
#     source_name: str,
#     provider_name: str,
#     access_token: str,
#     partition_start: dt.datetime,
#     partition_end: dt.datetime
# ) -> dict[str, Any]:

#     response = actions.unlock_partition(
#         source_name,
#         provider_name,
#         access_token,
#         partition_start,
#         partition_end,
#         partition_registry,
#         provider_registry,
#         source_registry
#     )

#     match response:
#         case FailedUnlock() as failed_response:
#             return HTTPException(HTTPStatus.CONFLICT, failed_response.error_message).__dict__
#         case SuccededUnlock() as succeded_response:
#             return PartitionUnlockResponse(
#                 status_code=HTTPStatus.OK,
#                 message="Succeded lock...",
#                 source=SimpleSource(source_name),
#                 provider=SimpleProvider(provider_name),
#                 partition=succeded_response.unlocked_object
#             ).__dict__


# @app.post("/sources/{source_name}/is_ready")
# def is_partition_ready(
#     source_name: str,
#     partition_start: dt.datetime,
#     partition_end: dt.datetime
# ) -> dict[str, Any]:

#     response = action.is_partition_ready(
#         source_name,
#         partition_start,
#         partition_end,
#         source_registry,
#         partition_registry
#     )

#     if response is True:
#         return PartitionReadyResponse(
#             status_code=HTTPStatus.OK,
#             message="Partition is ready...",
#             source=SimpleSource(source_name),
#         ).__dict__

#     if response is False:
#         return PartitionNotReadyResponse(
#             status_code=HTTPStatus.EXPECTATION_FAILED,
#             message="Partition is not ready...",
#             source=SimpleSource(source_name)
#         ).__dict__

#     return HTTPException(
#         HTTPStatus.INTERNAL_SERVER_ERROR,
#         f"Got unappropriate response: {type(response)}|{response}, contact the support team..."
#     ).__dict__
