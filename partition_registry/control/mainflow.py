import os
import datetime as dt
from typing import Any

from http import HTTPStatus

from fastapi import FastAPI
from fastapi import HTTPException

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from partition_registry import actions

from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry
from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import EventsRegistry

from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import PartitionNotReady
from partition_registry.data.status import PartitionReady

from partition_registry.data.response import RegistrationResponse
from partition_registry.data.response import PartitionResponse

from partition_registry.integration.postgres import init_postgres_session


app = FastAPI()

with init_postgres_session() as postgres_session:
    source_registry = SourceRegistry(postgres_session)
    provider_registry = ProviderRegistry(postgres_session)
    partition_registry = PartitionRegistry(postgres_session)
    events_registry = EventsRegistry(postgres_session)


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
            provider_registry,
            events_registry
        )
        match response:
            case FailedRegistration():
                return HTTPException(HTTPStatus.CONFLICT, response.error_message).__dict__
            case SuccededRegistration():
                return RegistrationResponse(HTTPStatus.OK, response.registered_object).__dict__


    @app.post("/partitions/unlock")
    def unlock_partition(
        start: dt.datetime,
        end: dt.datetime,
        source_name: str,
        provider_name: str
    ) -> dict[str, Any]:
        response = actions.unlock_partition(
            start,
            end,
            partition_registry,
            source_name,
            source_registry,
            provider_name,
            provider_registry,
            events_registry
        )
        match response:
            case FailedRegistration():
                return HTTPException(HTTPStatus.CONFLICT, response.error_message).__dict__
            case SuccededRegistration():
                return RegistrationResponse(HTTPStatus.OK, response.registered_object).__dict__


    @app.get("/sources/{source_name}/check_readiness")
    def check_partition_readiness(
        source_name: str,
        start: dt.datetime,
        end: dt.datetime,
    ) -> dict[str, Any]:
        response = actions.check_partition_readiness(
            start=start,
            end=end,
            source_name=source_name,
            source_registry=source_registry,
            events_registry=events_registry,
        )
        match response:
            case FailedRegistration():
                return HTTPException(HTTPStatus.CONFLICT, response.error_message).__dict__
            case PartitionNotReady() as not_ready:
                return PartitionResponse(HTTPStatus.OK, is_ready=False, message=not_ready.reason).__dict__
            case PartitionReady():
                return PartitionResponse(HTTPStatus.OK, is_ready=True).__dict__
