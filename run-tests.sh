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
echo "[run] Installing dev dependencies via uv..."
uv pip install --python .venv/bin/python -e ".[dev]"
echo "[run] Verifying 'rich' presence..."
.venv/bin/python -c 'import rich, sys; v=getattr(rich,"__version__","unknown"); print(f"[run] rich {v} detected")'

# Run tools from venv with unbuffered output; show test prints live
export PYTHONUNBUFFERED=1
export FORCE_COLOR=1
echo "[run] Running ruff..."
.venv/bin/ruff check
echo "[run] Running pytest (live output enabled)..."
.venv/bin/pytest -s -q
