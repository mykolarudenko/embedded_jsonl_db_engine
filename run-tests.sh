#!/usr/bin/env bash
set -euo pipefail

# Create venv via uv if available; fallback to python -m venv.
if command -v uv >/dev/null 2>&1; then
  # Create .venv if missing
  if [ ! -d ".venv" ]; then
    uv venv .venv
  fi
  # Install dev dependencies into the venv
  uv pip install --python .venv/bin/python -e ".[dev]"
  # Run tools from venv
  .venv/bin/ruff check
  .venv/bin/pytest -q
else
  # Fallback: standard venv + pip
  python3 -m venv .venv
  # shellcheck disable=SC1091
  . .venv/bin/activate
  python3 -m pip install -U pip
  pip install -e ".[dev]"
  ruff check
  pytest -q
fi
