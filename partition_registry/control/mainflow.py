import datetime as dt
from typing import Dict

from fastapi import FastAPI

from partition_registry.data.partition import Partition
from partition_registry.data.queue import MainQueue
from partition_registry.data.source import Source
from partition_registry.data.provider import Provider
from partition_registry.data.registry import 

app = FastAPI()
queue = MainQueue(100)
registry = 

@app.get("/")
def read_root() -> Dict[str, str]:
    message = "You are trying to get root page of Partition Registry Service. " \
        "Please, visit our documentation page (YOUR_URL/redocs) to get to know with complete functional"
    return {"message": message}


@app.post("/sources/{source.name}/register")
def register_source(source: Source) -> Dict[str, str]:
    


@app.post("/sources/{source.name}/providers/{provider.name}/partitions/lock")
def lock_partition(
    source: Source,
    provider: Provider,
    start: dt.datetime,
    end: dt.datetime
) -> Dict[str, str]:
    message = (
        "You are trying to get root page of Partition Registry Service. "
        "Please, visit our documentation page (YOUR_URL/redocs) to get to "
        "know with complete functional"
    )
    return {"message": message}


@app.post("/sources/{source}/providers/{provider}/partitions/unlock")
def lock_partition(
    source: Source,
    provider: Provider,
    start: dt.datetime,
    end: dt.datetime,
) -> Dict[str, str]:
    message = """You are trying to get root page of Partition Registry Service. Please, visit our documentation page (YOUR_URL/redocs) to get to know with complete functional"""
    return {"message": message}


# http://127.0.0.1:8000/registry/bigquery/sources/prodcloudna-de-production.registry.partition_registry/partitions/readiness/?partition_start=2020-01-01&partition_end=2020-02-01
@app.get("/registry/{source_type}/sources/{source}/partitions/readiness/")
def check_partition_readiness(
    source_type: str,
    source: str,
    partition_start: str,
    partition_end: str
) -> Dict[str, str | bool]:
    partition = Partition(partition_start, partition_end, dt.datetime.now())
    return {
        "source_type": source_type,
        "source": source,
        "partition": str(partition),
        "is_ready": True,
        "blocked_by": "NAME_OF_PROVIDER"  # TODO: ...
    }

@app.get("/queue")
def get_queue():
    return {"queue": [str(partition) for partition in queue.entries]}