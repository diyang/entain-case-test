#!/usr/bin/env bash
set -euo pipefail
set -x

test_output_dir="${TEST_OUTPUT_DIR:-test_outputs/unit}"
mkdir -p "${test_output_dir}"

uv run python -m unittest discover tests/unit -v 2>&1 | tee "${test_output_dir}/unittest.log"
