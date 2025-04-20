#!/bin/sh

echo "Running tests with pytest..."
pytest -v --color=yes --capture=no tests

echo "Checking code formatting with black..."
black --check --diff .

echo "Checking code types with mypy..."
mypy --pretty .

echo "Checking code style issues, logical errors, and bad practices with flake8..."
flake8 .

echo "All checks have passed"
