#!/usr/bin/env bash
# Full CDC demo: generate -> snapshot -> crash -> resume -> reconcile.
set -e
cd "$(dirname "$0")"
# Prefer the shared venv python (has pytest); fall back to system python3.
if [ -x "$HOME/workspace/ml-projects/venv/bin/python" ]; then
  PYTHON="$HOME/workspace/ml-projects/venv/bin/python"
else
  PYTHON="${PYTHON:-python3}"
fi
"$PYTHON" run.py
"$PYTHON" -m pytest tests/ -q
