#!/usr/bin/env bash

set -e

echo "Starting script execution..."

echo "Testing all files with mypy"
poetry run mypy --exclude 'flycheck_*' ./partition_registry --explicit-package-bases

echo "Testing tests with mypy"
poetry run mypy --exclude 'flycheck_*' ./tests --explicit-package-bases

echo "Running pytest..."
poetry run pytest --cov=partition_registry ./tests $@

echo "Running linter on module"
poetry run pylint --rcfile=setup.cfg --exit-zero ./partition_registry

echo "Running linter on tests"
poetry run pylint --rcfile=pylintrc_tests --exit-zero ./tests

echo "Script execution completed successfully!"
