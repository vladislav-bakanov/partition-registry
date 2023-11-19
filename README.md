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
