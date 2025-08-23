#!/usr/bin/env bash
set -euo pipefail

# Require uv; no fallbacks.
if ! command -v uv >/dev/null 2>&1; then
  echo "Error: 'uv' is required. Please install uv: https://github.com/astral-sh/uv" >&2
  exit 1
fi

# Create .venv if missing
if [ ! -d ".venv" ]; then
  uv venv .venv
fi

# Install dev dependencies into the venv
uv pip install --python .venv/bin/python -e ".[dev]"

# Run tools from venv
.venv/bin/ruff check
.venv/bin/pytest -q
