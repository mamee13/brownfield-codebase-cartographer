#!/usr/bin/env bash
set -e

echo "Running formatting checks..."
uv run ruff check .
uv run ruff format --check .

echo "Running type checks..."
uv run mypy src

echo "Running tests..."
uv run pytest -q

echo "All checks passed!"
