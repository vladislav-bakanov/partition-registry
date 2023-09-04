[![Built with Devbox](https://jetpack.io/img/devbox/shield_galaxy.svg)](https://jetpack.io/devbox/docs/contributor-quickstart/)


# Partition Registry
Platform-agnostic library to manage sources readiness.

# Problem that library solves
In case if you use several absolutely independent schedulers, but all of them should be syncronized and should know the same data - Partition Registry can help you.


# Prepare library for local developement:
1. Install [direnv](https://direnv.net/docs/installation.html)
1. Install [pyenv](https://pypi.org/project/pyenv/)
1. Make sure, that your are in activated env: `which python3` - it should be `.../partition-registry/.venv/bin/python3`
1. Install poetry: `pip3 install poetry==1.0.0`
1. Run environment instalation: `poetry install`
1. Enjoy


# Glossary

## Provider
Provider is entity, that can prepare some data source. Provider is needed to know entry point for the 

**Examples**:
1. You have 2 Airflow DAGs that fills a table in a data base. We have 2 different `Provider` objects
1. You have an `Airflow DAG`, that prepares table in a data base. This Airflow DAG is a `Provider` object


## Source
Source is any data source that can have state of readiness.

**Examples**:
1. You have 2 Airflow DAGs that fills a table in a data base. This table is a `Source` object
1. You have an Airflow DAG that fills a table in a data base. This table is a `Source` object


## Event
Event is a log-record for Source, which shows `Source` state and who made action for this `Source`.

**Examples**:
1. You have 2 Airflow DAGs that fills a table in a data base. Fact of readiness or not readiness of a `Source` is an `Event` object for each of DAGs
1. You have an Airflow DAG that fills a table in a data base. Fact of readiness or not readiness of a `Source` is an `Event` object