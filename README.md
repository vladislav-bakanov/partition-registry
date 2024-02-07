# Partition Registry
Platform-agnostic library to manage sources readiness.

# Glossary

**Partition** - time interval for some data source.


# What this module can do
This module allows you to do 3 things:
1. Lock Partitions - if you locked a Partition - anyone who wants to work with this source and has a dependency of this source will know that source data is in progress for specified time interval (Partition).
1. Unlock Partitions - unlock the Source and make this source availiable to work with it.
1. Get to know wheter a source is ready or not


# When it could be useful
1. When you have distributed Airflow. For example, just imagine that you're a Data Engineer and you have 2 absolutelly isolated Airflow instances, but each of them should know the state of the tasks from the other one.
2. When you have crono-jobs, that update data simulteniously and you want to know that data is ready for certain time interval.


# Get started
1. [Install direnv](https://direnv.net/docs/installation.html)
1. [Install pyenv](https://ggkbase-help.berkeley.edu/how-to/install-pyenv/)
1. Run in terminal: `direnv allow`. It should show you something like: `...direnv: export +PROJECT_ROOT +PYENV_VERSION +PYTHONPATH ~PATH`
1. Go to `partition-registry` folder
1. Ensure, that your `python3` is located in local `.venv` folder: `which python3` -> should return `.../partition-registry/.venv/bin/python3`
1. Install dependencies with `poetry` by the command: `poetry install`
1. To ensure, that everything works, run tests: `./test.sh`


# Features
- Source can be registered only once and should be registered via `/sources/register`. After the registration you will get `access_token` in a response.
Please store this `access_token` somewhere, because only by this token you can access, lock and unlock it.


# Take into account:
For now service works only in-memory (because it's just a POC), but i'm driving this project into stable service state.


# Example:
Register a source, called `some_source` by the owner `some_owner`.

```
curl -X 'POST' \
  'http://localhost:5498/sources/register?source_name=some_source&owner=some_owner' \
  -H 'accept: application/json' \
  -d ''
```

**Command output will contain an access key for this source. You can't access to the source without this key. Don't loose it**

Register provider, called `some_provider`, with the access key. Specify `access_token` in the URL (for example: `access_token=e7bbc037-36e7-4305-8b76-11c997f1b64b`):

```
curl -X 'POST' \
  'http://localhost:5498/providers/register?provider_name=some_provider&access_token=' \
  -H 'accept: application/json' \
  -d ''
```

Then register a partition, you interested by the provider and the source.
In this example we register a partition for the period `[2023-01-01T00:00:00 - 2023-01-02T00:00:00]` for the source `some_source` and provided by provider `some_provider`:

```
curl -X 'POST' \
  'http://localhost:5498/partitions/register?start=2023-01-01%2000%3A00%3A00&end=2023-01-02%2000%3A00%3A00&source_name=some_source&provider_name=some_provider' \
  -H 'accept: application/json' \
  -d ''
```

Now we did everything to start to manipulate with events:

```
curl -X 'POST' \
  'http://localhost:5498/partitions/lock?start=2023-01-01%2000%3A00%3A00&end=2023-01-02%2000%3A00%3A00&source_name=wm_emails&provider_name=wm_emails_dag' \
  -H 'accept: application/json' \
  -d ''
```