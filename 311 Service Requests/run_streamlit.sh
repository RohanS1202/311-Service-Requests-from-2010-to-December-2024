#!/usr/bin/env bash
# Convenience launcher to run the Streamlit app from repo root
# Prefer using .venv312 if present (created by the setup script); otherwise fall back to system streamlit.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT_DIR/311 Service Requests/app_streamlit.py"
LOGFILE="/tmp/streamlit_app.log"
# allow overriding port via env var, default to 8501
PORT=${STREAMLIT_PORT:-8501}
# prefer .venv312 python if available
if [ -x "$ROOT_DIR/.venv312/bin/python" ]; then
	echo "Starting Streamlit using .venv312 (python: $ROOT_DIR/.venv312/bin/python)"
	nohup "$ROOT_DIR/.venv312/bin/python" -m streamlit run "$SCRIPT" --server.fileWatcherType none --server.port "$PORT" "$@" > "$LOGFILE" 2>&1 &
	echo $! > /tmp/streamlit_pid312.txt
	echo "Streamlit started (pid $(cat /tmp/streamlit_pid312.txt)), logs: $LOGFILE"
else
	echo "Starting Streamlit using system python/streamlit"
	nohup python -m streamlit run "$SCRIPT" --server.fileWatcherType none --server.port "$PORT" "$@" > "$LOGFILE" 2>&1 &
	echo $! > /tmp/streamlit_pid.txt
	echo "Streamlit started (pid $(cat /tmp/streamlit_pid.txt)), logs: $LOGFILE"
fi
