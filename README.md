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
