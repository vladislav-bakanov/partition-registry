# Partition Registry Documentation

## Introduction
Partition Registry is a platform-independent library designed for managing sources, providers, and partitions in various real-world scenarios. It is particularly useful for handling data storage and retrieval in a structured and efficient manner.

## Key Concepts

### Glossary
- **Source**: A place where data is stored, like a database table or an Excel file.
- **Provider**: An entity that supplies data to a source, such as an Airflow DAG or a Kubernetes job.
- **Partition**: A specific time interval associated with a data source.

### Use Cases
1. **Distributed Airflow Management**: Useful for Data Engineers managing isolated Airflow instances needing to be aware of each other's task states.
2. **Cron Job Coordination**: Helps in monitoring data updates happening simultaneously across multiple cron jobs, ensuring data readiness for specific time intervals.

## Getting Started

### Quick Setup with Docker
1. Install Docker from [Docker's official site](https://www.docker.com/).
2. Navigate to the root folder: `./partition-registry`.
3. Execute `docker compose up`.

Docker will start three containers:
- PostgreSQL (default URL: http://localhost:5432/)
- Adminer (default URL: http://localhost:8080/)
- Web Application (default URL: http://localhost:5498/)

Adminer or any other DBMS can be used to view all created entities. Documentation for the Web Service is available at: http://localhost:5498/docs.

### Local Development Setup
1. Install `direnv` from [Direnv's official site](https://direnv.net/docs/installation.html).
2. Install `pyenv` following instructions at [Berkeley's GGKBase](https://ggkbase-help.berkeley.edu/how-to/install-pyenv/).
3. Go to the `partition-registry` folder.
4. Run `direnv allow` in the terminal. This step installs poetry and the required Python version.
5. Verify the `python3` path with `which python3` - it should point to `.../partition-registry/.venv/bin/python3`.

## Core Interfaces
1. **Source Registration**: Register your source to receive an access key for data provision.
   - Endpoint: `/sources/register`
2. **Provider Registration**: Register a provider using an `access_token` to ensure authorized access to a source.
   - Endpoint: `/providers/register`
3. **Partition Registration**: Register partitions as atomic parts of the system.
   - Endpoint: `/partitions/register`
4. **Partition Lock/Unlock**: Lock or unlock partitions to manage data availability.
   - Lock Endpoint: `/partitions/lock`
   - Unlock Endpoint: `/partitions/unlock`
5. **Check Readiness**: Verify if data in a source is ready for a specific period.
   - Endpoint: `/sources/{source_name}/check_readiness`


### Source Registration

- Initial registration will create an object with Access Token.
- All following attempts to register source won't create an entity and won't provide Access Keys

```python
import requests
from urllib.parse import urljoin

WEB_SERVICE_URL = "http://127.0.0.1:5498"
SOURCE_NAME = 'public.some_source'
SOURCE_OWNER = "ivan.ivanov@company.com"

response = requests.post(
    urljoin(WEB_SERVICE_URL, 'sources/register'),
    params={"source_name": SOURCE_NAME, "owner": SOURCE_OWNER}
)

status_code = response.status_code
data = response.json()
```

### Provider Registration

- Access Key should be the same as specified after Source registration to have an access to this Source.

```python
import requests
from urllib.parse import urljoin

WEB_SERVICE_URL = "http://127.0.0.1:5498"
PROVIDER_NAME = 'provider@email.com'
ACCESS_TOKEN = '...' #  <--- Your Key to access the Source

response = requests.post(
    urljoin(WEB_SERVICE_URL, 'providers/register'),
    params={"provider_name": PROVIDER_NAME, "access_token": ACCESS_TOKEN}
)

status_code = response.status_code
data = response.json()
```

### Partition Registration

- Partition should be registered to start work with it
- If timezone is not provided - object will be converted to the timestamp with tz=UTC
- To Register Partition all entities like Source, Provider should be registered

```python
import datetime as dt
from datetime import timedelta
from dateutil import tz

import requests
from urllib.parse import urljoin

WEB_SERVICE_URL = "http://127.0.0.1:5498"
PARTITION_START = dt.datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=tz.UTC)
PARTITION_END = dt.datetime(2000, 1, 2, 0, 0, 0, 0, tzinfo=tz.UTC)
SOURCE_NAME = 'public.some_source'
PROVIDER_NAME = 'provider@email.com'

response = requests.post(
    urljoin(WEB_SERVICE_URL, 'partitions/register'),
    params={
        "start": str(PARTITION_START),
        "end": str(PARTITION_END),
        "source_name": SOURCE_NAME,
        "provider_name": PROVIDER_NAME,
    }
)

status_code = response.status_code
data = response.json()
```

### Partition Lock | Unlock

- Partition can be locked or unlocked
- Partition shoul be registered before lock or unlock action
- If partition is locked for specific interval - all dependens will be notified that Source is not ready to consume for this period.

```python
import datetime as dt
from datetime import timedelta
from dateutil import tz

import requests
from urllib.parse import urljoin

WEB_SERVICE_URL = "http://127.0.0.1:5498"
PARTITION_START = dt.datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=tz.UTC)
PARTITION_END = dt.datetime(2000, 1, 2, 0, 0, 0, 0, tzinfo=tz.UTC)
SOURCE_NAME = 'public.some_source'
PROVIDER_NAME = 'provider@email.com'

lock_response = requests.post(
    urljoin(WEB_SERVICE_URL, 'partitions/unlock'),
    params={
        "start": str(PARTITION_START),
        "end": str(PARTITION_END),
        "source_name": SOURCE_NAME,
        "provider_name": PROVIDER_NAME,
    }
)

unlock_response = requests.post(
    urljoin(WEB_SERVICE_URL, 'partitions/unlock'),
    params={
        "start": str(PARTITION_START),
        "end": str(PARTITION_END),
        "source_name": SOURCE_NAME,
        "provider_name": PROVIDER_NAME,
    }
)

status_code = unlock_response.status_code
lock_data = unlock_response.json()

status_code = lock_response.status_code
unlock_data = lock_response.json()
```

### Check Readiness

```python
import datetime as dt
from datetime import timedelta
from dateutil import tz

import requests
from urllib.parse import urljoin

WEB_SERVICE_URL = "http://127.0.0.1:5498"
PARTITION_START = dt.datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=tz.UTC)
PARTITION_END = dt.datetime(2000, 1, 2, 0, 0, 0, 0, tzinfo=tz.UTC)
SOURCE_NAME = 'public.some_source'
PROVIDER_NAME = 'provider@email.com'

response = requests.get(
    urljoin(WEB_SERVICE_URL, f'sources/{SOURCE_NAME}/check_readiness'),
    params={
        "source_name": str(SOURCE_NAME),
        "start": str(PARTITION_START),
        "end": str(PARTITION_END),
    }
)

status_code = response.status_code
data = response.json()
```

