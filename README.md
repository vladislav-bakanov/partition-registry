# Partition Registry
Platform-agnostic library to manage sources readiness.

# Problem that library solves
In case if you use several absolutely independent schedulers, but all of them should be syncronized and should know the same data - Partition Registry can help you.


# Prepare library for local developement:
1. Do direnv allow (ensure, that direnv is already installed)
1. Ensure that you use pip from `partition-registry` project: `which python3` -> should be .../partition-registry/.venv/bin/python3
1. Run poetry installation: `pip3 install poetry==1.0.0`
1. Install dependencies with poetry: `poetry install`
1. To ensure, that everything works, run tests: `./test.sh`
