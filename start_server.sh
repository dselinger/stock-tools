#!/usr/bin/env bash
set -euo pipefail

# --- Paths ---
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
APP_DIR="$SCRIPT_DIR"           # where app.py lives
# Prefer project-local venv by default; override by exporting VENV_PATH if needed
VENV="${VENV_PATH:-$SCRIPT_DIR/.venv}"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/.env}"

# --- Load .env (export all vars) ---
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +a
else
  echo "⚠️  No .env found at $ENV_FILE (continuing; expecting MASSIVE_API_KEY or POLYGON_API_KEY in env)"
fi

# --- Sanity checks ---
DEMO_MODE="${DEMO_MODE:-0}"
if [[ "$DEMO_MODE" != "1" ]]; then
  : "${MASSIVE_API_KEY:=${POLYGON_API_KEY:-}}"
  : "${MASSIVE_API_KEY:?Set MASSIVE_API_KEY (or POLYGON_API_KEY) in your environment/.env (or set DEMO_MODE=1)}"
else
  echo "ℹ️  DEMO_MODE=1 — skipping Massive API key checks (serving bundled sample data)."
fi
if [[ ! -d "$VENV" ]]; then
  echo "❌ Virtual env not found at $VENV"
  echo "   Create it with:  python3 -m venv \"$VENV\""
  echo "   Then activate and install deps:  source \"$VENV/bin/activate\" && pip install fastapi uvicorn httpx numpy pandas itsdangerous jinja2 python-multipart yfinance"
  exit 1
fi
if [[ ! -f "$APP_DIR/app.py" ]]; then
  echo "❌ app.py not found in $APP_DIR"
  exit 1
fi

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
# Concurrency knobs (override in .env or env vars)
SCANNER_CONCURRENCY="${SCANNER_CONCURRENCY:-8}"
POLYGON_CONCURRENCY="${POLYGON_CONCURRENCY:-8}"

# --- Activate venv ---
# shellcheck disable=SC1090
source "$VENV/bin/activate"

# --- Create cache dir if missing ---
mkdir -p "$APP_DIR/cache" "$APP_DIR/cache/gex_disk" "$APP_DIR/cache/gex_history" "$APP_DIR/cache/gex_trend"

# --- Launch ---
echo "▶️  Starting Dave's Stock Tools at http://${HOST}:${PORT}/"
echo "    App dir:     $APP_DIR"
echo "    Using venv:  $VENV"
echo "    .env file:   $ENV_FILE"
echo "    Scanner fan-out: ${SCANNER_CONCURRENCY} workers"
echo "    Massive (Polygon) fan-out: ${POLYGON_CONCURRENCY} workers"

export SCANNER_CONCURRENCY
export POLYGON_CONCURRENCY

# Run from APP_DIR so uvicorn can import the app cleanly
cd "$APP_DIR"

# Tip: add --reload for dev auto-reload
exec uvicorn app:app --host "$HOST" --port "$PORT"
