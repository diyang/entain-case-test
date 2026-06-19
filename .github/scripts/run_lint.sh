#!/usr/bin/env bash
set -euo pipefail
set -x

lint_output_dir="${LINT_OUTPUT_DIR:-test_outputs/lint}"
mkdir -p "${lint_output_dir}"

uv run ruff format --check --verbose src tests 2>&1 | tee "${lint_output_dir}/ruff-format.log"
uv run ruff check --verbose src tests 2>&1 | tee "${lint_output_dir}/ruff-check.log"
