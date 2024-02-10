import datetime as dt
from typing import Any

from http import HTTPStatus

from fastapi import FastAPI
from fastapi import HTTPException

from partition_registry.actions.register_source import register_source as rsource
from partition_registry.actions.register_provider import register_provider as rprovider
from partition_registry.actions.register_partition import register_partition as rpartition
from partition_registry.actions.lock_partition import lock_partition as lpartition
from partition_registry.actions.unlock_partition import unlock_partition as upartition
from partition_registry.actions.check_partition_readiness import check_partition_readiness as check_readiness

from partition_registry.actor.source_registry import SourceRegistry
from partition_registry.actor.provider_registry import ProviderRegistry
from partition_registry.actor.partition_registry import PartitionRegistry
from partition_registry.actor.events_registry import EventsRegistry

from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import PartitionNotReady
from partition_registry.data.status import PartitionReady

from partition_registry.data.response import SucceededRegistrationResponse
from partition_registry.data.response import PartitionReadinessResponse

from partition_registry.data.func import localize

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
        """Register source to manage within the Partition Registry service

        Args:
            source_name (str): source name to register
            owner (str): source owner

        Returns:
            HTTPException(HTTPStatus.CONFLICT)
            SucceededRegistrationResponse(HTTPStatus.OK, RegisteredSource)
        """
        response = rsource(source_name, owner, source_registry)
        match response:
            case FailedRegistration():
                return HTTPException(HTTPStatus.CONFLICT, response.message).__dict__
            case SuccededRegistration() as success:
                return SucceededRegistrationResponse(HTTPStatus.OK, success.obj).__dict__


    @app.post("/providers/register")
    def register_provider(provider_name: str, access_token: str) -> dict[str, Any]:
        """Register provider to manage it within the Partition Registry service

        Args:
            provider_name (str): provider name to register
            access_token (str): access token to get access to the source

        Returns:
            HTTPException(HTTPStatus.CONFLICT)
            SucceededRegistrationResponse(HTTPStatus.OK, RegisteredProvider)
        """
        response = rprovider(provider_name, access_token, provider_registry)
        match response:
            case FailedRegistration():
                return HTTPException(HTTPStatus.CONFLICT, response.message).__dict__
            case SuccededRegistration() as success:
                return SucceededRegistrationResponse(HTTPStatus.OK, success.obj).__dict__


    @app.post("/partitions/register")
    def register_partition(
        start: dt.datetime,
        end: dt.datetime,
        source_name: str,
        provider_name: str
    ) -> dict[str, Any]:
        """Register partition to manage it within Partition Registry

        Args:
            start (dt.datetime): startpoint of partition to register
            end (dt.datetime): endpoint of partition to register
            source_name (str): source to register partition
            provider_name (str): provider to register partition

        Returns:
            HTTPException(HTTPStatus.CONFLICT)
            SucceededRegistrationResponse(HTTPStatus.OK, RegisteredPartition)
        """
        start = localize(start)
        end = localize(end)
        response = rpartition(
            start=start,
            end=end,
            partition_registry=partition_registry,
            source_name=source_name,
            source_registry=source_registry,
            provider_name=provider_name,
            provider_registry=provider_registry
        )
        match response:
            case FailedRegistration():
                return HTTPException(HTTPStatus.CONFLICT, response.message).__dict__
            case SuccededRegistration() as success:
                return SucceededRegistrationResponse(HTTPStatus.OK, success.obj).__dict__


    @app.post("/partitions/lock")
    def lock_partition(
        start: dt.datetime,
        end: dt.datetime,
        source_name: str,
        provider_name: str
    ) -> dict[str, Any]:
        """Lock registered partition

        Args:
            start (dt.datetime): startpoint of partition to lock
            end (dt.datetime): endpoint of partition to lock
            source_name (str): source to lock
            provider_name (str): provider that locks the interval

        Returns:
            HTTPException(HTTPStatus.CONFLICT)
            SucceededRegistrationResponse(HTTPStatus.OK, RegisteredPartitionEvent)
        """
        start = localize(start)
        end = localize(end)

        response = lpartition(
            start=start,
            end=end,
            partition_registry=partition_registry,
            source_name=source_name,
            source_registry=source_registry,
            provider_name=provider_name,
            provider_registry=provider_registry,
            events_registry=events_registry
        )
        match response:
            case FailedRegistration():
                return HTTPException(HTTPStatus.CONFLICT, response.message).__dict__
            case SuccededRegistration() as success:
                return SucceededRegistrationResponse(HTTPStatus.OK, success.obj).__dict__


    @app.post("/partitions/unlock")
    def unlock_partition(
        start: dt.datetime,
        end: dt.datetime,
        source_name: str,
        provider_name: str
    ) -> dict[str, Any]:
        """Unlock registered partition

        Args:
            start (dt.datetime): startpoint of partition to unlock
            end (dt.datetime): endpoint of partition to unlock
            source_name (str): source to unlock
            provider_name (str): provider that unlocks the interval

        Returns:
            HTTPException(HTTPStatus.CONFLICT)
            SucceededRegistrationResponse(HTTPStatus.OK, RegisteredPartitionEvent)
        """
        start = localize(start)
        end = localize(end)

        response = upartition(
            start=start,
            end=end,
            partition_registry=partition_registry,
            source_name=source_name,
            source_registry=source_registry,
            provider_name=provider_name,
            provider_registry=provider_registry,
            events_registry=events_registry
        )
        match response:
            case FailedRegistration():
                return HTTPException(HTTPStatus.CONFLICT, response.message).__dict__
            case SuccededRegistration() as success:
                return SucceededRegistrationResponse(HTTPStatus.OK, success.obj).__dict__


    @app.get("/sources/{source_name}/check_readiness")
    def check_partition_readiness(
        source_name: str,
        start: dt.datetime,
        end: dt.datetime,
    ) -> dict[str, Any]:
        """Check source partition readiness

        Args:
            source_name (str): source to check
            start (dt.datetime): startpoint of partition to check
            end (dt.datetime): end of partition to check

        Returns:
            PartitionReadinessResponse(HTTPStatus.OK, True/False, message)
        """
        start = localize(start)
        end = localize(end)

        response = check_readiness(
            start=start,
            end=end,
            source_name=source_name,
            partition_registry=partition_registry,
            events_registry=events_registry
        )
        match response:
            case PartitionNotReady() as not_ready:
                return PartitionReadinessResponse(HTTPStatus.OK, is_ready=False, message=not_ready.reason).__dict__
            case PartitionReady():
                return PartitionReadinessResponse(HTTPStatus.OK, is_ready=True).__dict__
