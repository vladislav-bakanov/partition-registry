import datetime as dt
from typing import Dict

from fastapi import FastAPI

from partition_registry.data.partition import UnknownPartition
from partition_registry.actor.queue import MainQueue
from partition_registry.data.source import Source
from partition_registry.data.provider import Provider

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
def read_root() -> Dict[str, str]:
    message = (
        "You are trying to get root page of Partition Registry Service. "
        f"Please, visit documentation page by address {app.redoc_url} to get to "
        "know with complete functional"
    )
    return {"message": message}


@app.post("/sources/register/{source}/{provider}")
def register_source(
    source: str,
    provider: str
) -> Dict[str, str]:
    return {"message": "Not implemented yet"}

@app.post("/sources/{source.name}/providers/{provider.name}/partitions/lock")
def lock_partition(
    source: str,
    provider: str,
    start: dt.datetime,
    end: dt.datetime
) -> Dict[str, str]:
    message = (
        "You are trying to get root page of Partition Registry Service. "
        f"Please, visit documentation page by address {app.redoc_url} to get to "
        "know with complete functional"
    )
    return {"message": message}


@app.post("/sources/{source}/providers/{provider}/partitions/unlock")
def unlock_partition(
    source: str,
    provider: str,
    start: dt.datetime,
    end: dt.datetime,
) -> Dict[str, str]:
    message = """You are trying to get root page of Partition Registry Service. Please, visit our documentation page (YOUR_URL/redocs) to get to know with complete functional"""
    return {"message": message}


# http://127.0.0.1:8000/registry/bigquery/sources/prodcloudna-de-production.registry.partition_registry/partitions/readiness/?partition_start=2020-01-01&partition_end=2020-02-01
@app.get("/registry/source/{source_name}/partitions/is_ready/")
def check_partition_readiness(
    source_name: str,
    partition_start: dt.datetime,
    partition_end: dt.datetime,
) -> Dict[str, str | bool]:
    partition = UnknownPartition(partition_start, partition_end)
    return {
        "source": source_name,
        "partition": str(partition),
        "is_ready": True,
        "blocked_by": "NAME_OF_PROVIDER"  # TODO: ...
    }

@app.get("/queue")
def get_queue() -> dict[str, list[str]]:
    return {"queue": [str(partition) for partition in queue.records]}
