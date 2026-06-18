#!/usr/bin/env bash
set -euo pipefail
set -x

uv run ruff format --check --verbose src tests
uv run ruff check --verbose src tests
