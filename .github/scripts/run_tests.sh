#!/usr/bin/env bash
set -euo pipefail
set -x

uv run python -m unittest discover tests -v
