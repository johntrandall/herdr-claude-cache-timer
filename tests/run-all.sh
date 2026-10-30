#!/bin/sh
# The gate: compile, then the unit and ticker tests.
set -e
cd "$(dirname "$0")/.."
"${PYTHON:-python3}" -m py_compile claude_cache_timer.py
"${PYTHON:-python3}" -m unittest discover -s tests -v
