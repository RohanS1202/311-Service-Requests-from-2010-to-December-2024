#!/usr/bin/env bash
# setup_local.sh — create venv, install deps, precompute summaries and run health check
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

VENV=.venv312
PY=$VENV/bin/python
PIP=$VENV/bin/pip

if [ ! -d "$VENV" ]; then
  echo "Creating virtualenv $VENV using python3.12"
  python3.12 -m venv "$VENV"
fi

echo "Upgrading pip and installing requirements"
$PY -m pip install --upgrade pip
$PIP install -r "311 Service Requests/requirements.txt"

echo "Running precompute summaries"
$PY scripts/precompute_summaries.py --sla 24

echo "Running health check"
$PY scripts/health_check.py

echo "Setup complete. To run the app locally:"
echo "  ./\"311 Service Requests\"/run_streamlit.sh"
