#!/usr/bin/env bash
# refresh_summaries.sh — run precompute in a clean venv environment
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
VENV=.venv312
if [ ! -x "$VENV/bin/python" ]; then
  echo "Virtualenv not found: $VENV. Run setup_local.sh first." >&2
  exit 2
fi
$VENV/bin/python scripts/precompute_summaries.py --sla 24
