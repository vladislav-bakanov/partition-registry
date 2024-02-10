import datetime as dt
from datetime import timedelta
from dateutil import tz

from typing import Any
import requests
from http import HTTPStatus
from urllib.parse import urljoin
from pprint import pprint as pp


WEB_SERVICE_URL = "http://127.0.0.1:5498"
SOURCE_NAME = 'public.some_source1'
SOURCE_OWNER = "ivan.ivanov@company.com"
PROVIDER_NAME = "my_provider"
PARTITION_START = dt.datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=tz.UTC)
PARTITION_END = dt.datetime(2000, 1, 2, 0, 0, 0, 0, tzinfo=tz.UTC)


pp("Partition Registry Service is a service to manage Sources, Partitions and Providers")
pp("Partition is ")
# if this source is already exist, it won't do anything
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
    pp(registered_object)
    access_token_obj = registered_object.get('access_token') or {}
    access_token = access_token_obj.get('token')
    
    pp(f"You successfully registered source: {registered_source_data.get('name')}")
    pp(f"To access this source, lock and ulock partitions, use this access_token {access_token}")


# If we going to do it one more time, we will get code 409, because source is already exist
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
