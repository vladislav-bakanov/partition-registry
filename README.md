# Partition Registry
Platform-agnostic library to manage sources, providers and partitions in real world.

# Glossary

**Source** - is a entity, where data can be stored. It can be a database table, excel-file, anything that can provide data to real world.
**Provider** - is an entity, that can provide data to source. It can be some Airflow DAG, k8s job or anything that can provide data to the source.
**Partition** - time interval for some data source


# When could it be useful
1. When you have distributed Airflow. Just imagine that you're a Data Engineer and you have 2 absolutelly isolated Airflow instances, but each of them should know the state of the tasks from the other one - here Partition Registry comes in handy.
2. When you have plenty of crono-jobs, that update data simulteniously and you want to know that data is ready for certain time interval.


# Get started

## To just run
1. [Install docker](https://www.docker.com/) - it'll allow you to run whole service with all it's dependencies in one command.
1. Go to the root folder: `./partition-registry`
1. Run `docker compose up`

Docker should up 3 containers:
- PostgreSQL (by default: http://localhost:5432/)
- Adminer (by default: http://localhost:8080/)
- Web Application (by default: http://localhost:5498/)

You can check all created entities with the help of adminer or any other DBMS.
All docs for the Web Service is available: http://localhost:5498/docs (by default)


## For local development
1. [Install direnv](https://direnv.net/docs/installation.html) - it'll allow you to work with this project smoothly and will activate invironment when you enter project folder.
1. [Install pyenv](https://ggkbase-help.berkeley.edu/how-to/install-pyenv/)
1. Go to `partition-registry` folder
1. Run in terminal: `direnv allow`. It should show you something like: `...direnv: export +PROJECT_ROOT +PYENV_VERSION +PYTHONPATH ~PATH`. On this step `pyenv` will install poetry and required python version.  
1. Be sure, that your `python3` is located in local `.venv` folder: `which python3` -> should return `.../partition-registry/.venv/bin/python3`


# Core interfaces
1. `Source Registration` - as far as `Source` is a public interface for the End User to consume the data, we should carefully threat this asset. That's why you should register your `Source` within the system first. After **initial** `Source` registration **you will receive an access key** to provide any data to this Source.
It won't be shown during the same source registration process.

**Interface to use**: `/sources/register`

1. `Provider Registration` - `Provider` is a core asset to provide data to the `Source`. As far as the `Source` it also should be registered within the system. To don't violate the privacy of someone's `Source` we have to use `access_token` to be sure, that provider has an access to the `Source`.

**Interface to use**: `/providers/register`

1. `Partition Registration` - `Partition` is an atomic part of Partition Registry. Every `Source` consists of plenty of `Partitions` provided by defferent `Provider`s. Before work with partitions - register it within the system.

**Interface to use**: `/partitions/register`

1. `Partition Lock` - `Partition` can be locked to notify all dependens that specific interval for the `Source` data is unavailable to consume.

**Interface to use**: `/partitions/lock`

1. `Partition Unlock` - `Partition` can be unlocked to notify all dependens that specific interval for the `Source` data is available to consume.

**Interface to use**: `/partitions/unlock`

1. `Check Readiness` - is used to know that `Source` data is available to consume for specific period.

**Interface to use**: `/sources/{source_name}/check_readiness`

# Take into account:
For now service works only in-memory (because it's just a POC), but i'm driving this project into stable service state.

# Example:
```
from typing import Any

import datetime as dt

from datetime import timedelta
from dateutil import tz

import requests

from http import HTTPStatus
from urllib.parse import urljoin
from pprint import pprint as pp


WEB_SERVICE_URL = "http://127.0.0.1:5498"
SOURCE_NAME = 'public.some_source'
SOURCE_OWNER = "ivan.ivanov@company.com"
PROVIDER_NAME = "public_provider"
PARTITION_START = dt.datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=tz.UTC)
PARTITION_END = dt.datetime(2000, 1, 2, 0, 0, 0, 0, tzinfo=tz.UTC)


# Let's register our first Source
source_reg_response = requests.post(
    urljoin(WEB_SERVICE_URL, 'sources/register'),
    params={
        "source_name": SOURCE_NAME,
        "owner": SOURCE_OWNER
    }
)

if source_reg_response.status_code == HTTPStatus.OK:
    registered_source_data = source_reg_response.json()
    registered_object: dict[str, Any] = registered_source_data.get('registered_object') or {}
    access_token_obj = registered_object.get('access_token') or {}
    access_token = access_token_obj.get('token')
    pp(f"You successfully registered source: {registered_object}")
    pp(f"To access this source, lock and ulock partitions, use this access_token {access_token}")


# If you're going to register source one more time - you will get code 409, because source is already exist
# Check it
source_reg_response_attempt_2 = requests.post(
    urljoin(WEB_SERVICE_URL, 'sources/register'),
    params={
        "source_name": SOURCE_NAME,
        "owner": SOURCE_OWNER
    }
)
pp(f"Now responce code is: {source_reg_response_attempt_2.status_code}")


# To fill the source by data we should have at least one provider
# Let's create our first provider
# But to be sure, that source can be filled only by authorized providers
# you should specify access key for the source while creating provider
# 
# BTW, there can be more than one provider by the source
provider_reg_response = requests.post(
    urljoin(WEB_SERVICE_URL, 'providers/register'),
    params={
        "provider_name": PROVIDER_NAME,
        "access_token": access_token,  # This token we've gotten after source registration
    }
)

# Now we registered provider within the system, let's print the result
if provider_reg_response.status_code == HTTPStatus.OK:
    pp(provider_reg_response.json())


# Well, now we have provider and source, but we don't have any partition by this provider to manage it within source
# But before manage partition we should register this partition within the system
partition_reg_response = requests.post(
    urljoin(WEB_SERVICE_URL, 'partitions/register'),
    params={
        "start": str(PARTITION_START),
        "end": str(PARTITION_END),
        "source_name": SOURCE_NAME,
        "provider_name": PROVIDER_NAME,
    }
)

# Now we registered provider within the system, let's print the result
if partition_reg_response.status_code == HTTPStatus.OK:
    pp(partition_reg_response)


# We registered source, provider and partition within the system
# Now it's time to create events by this registered partition
# Events can be:
# - LOCK
# - UNLOCK
# 
# Let's UNLOCK partition first
unlock_partition_response = requests.post(
    urljoin(WEB_SERVICE_URL, 'partitions/unlock'),
    params={
        "start": str(PARTITION_START),
        "end": str(PARTITION_END),
        "source_name": SOURCE_NAME,
        "provider_name": PROVIDER_NAME,
    }
)

# Now we unlocked this partition by the source
if unlock_partition_response.status_code == HTTPStatus.OK:
    pp(unlock_partition_response.json())

# We can also LOCK partition, but it's absolutely the same procedure as UNLOCK, but
# by path: /partitions/lock
# 
# But more interesting thing for us - to know is some interval ready to be consumed or not
# To do this let's use: /sources/{source_name}/check_readiness

# Let's check the interval, that less, than our initial partition
check_readiness_response = requests.get(
    urljoin(WEB_SERVICE_URL, f'sources/{SOURCE_NAME}/check_readiness'),
    params={
        "source_name": str(SOURCE_NAME),
        "start": str(PARTITION_START),
        "end": str(PARTITION_END - timedelta(hours=1)),
    }
)

if check_readiness_response.status_code:
    pp(check_readiness_response.json())
    
# Well let's check the interval, that greater, than our initial partition
check_readiness_response = requests.get(
    urljoin(WEB_SERVICE_URL, f'sources/{SOURCE_NAME}/check_readiness'),
    params={
        "source_name": str(SOURCE_NAME),
        "start": str(PARTITION_START),
        "end": str(PARTITION_END + timedelta(hours=1)),
    }
)

if check_readiness_response.status_code:
    pp("And now our partition is not ready...")
    pp(check_readiness_response.json())

```