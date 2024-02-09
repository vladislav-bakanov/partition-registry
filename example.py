from typing import Any
import requests
from http import HTTPStatus
from urllib.parse import urljoin
from pprint import pprint as pp
import json

WEB_SERVICE_URL = "http://127.0.0.1:5498"

# This function will safely register a source
# if this source is already exist, it won't do anything
source_reg_response = requests.post(
    urljoin(WEB_SERVICE_URL, 'sources/register'),
    params={"source_name": "public.wm_emails"}
)

# After registration response will contain
data: dict[str, Any] = source_reg_response.json()
source: dict[str, Any] = data.get('source', {})
access_token = source.get('access_token', dict()).get('token')

if source_reg_response.status_code == HTTPStatus.OK:
    pp(f"You successfully registered source: {source.get('name')}")
    pp(f"To access this source, lock and ulock partitions, use this access_token {access_token}")

lock_partition_response = requests.post(
    urljoin(BASE_URL, 'sources/My First Source/My Provider/lock'),
    params={
        "partition_start": "2000-01-01T00:00:00",
        "partition_end": "2000-01-02T00:00:00",
        "access_token": access_token,
    }
)
d = lock_partition_response.json()
if lock_partition_response.status_code == 200:
    pp(f"You successfully locked partition: {d}")