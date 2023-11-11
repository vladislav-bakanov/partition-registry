#!/usr/bin/env bash

set -e

echo "Starting script execution..."

echo "Testing all files with mypy"
poetry run mypy --exclude 'flycheck_*' ./partition_registry

echo "Running pytest..."
poetry run pytest --cov=partition_registry ./tests $@

echo "Running linter"
poetry run pylint --rcfile=setup.cfg --exit-zero ./partition_registry

echo "Script execution completed successfully!"
