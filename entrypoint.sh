#!/usr/bin/env bash

: "${MODAL_TOKEN_ID:?Set MODAL_TOKEN_ID}"
: "${MODAL_TOKEN_SECRET:?Set MODAL_TOKEN_SECRET}"

if [[ -n "${NEON_DATABASE_URL:-}" ]]; then
  modal secret create neon-dsn NEON_DATABASE_URL="${NEON_DATABASE_URL}" --force
fi

modal run /app.py --keywords "${KEYWORDS}" --geos "${GEOS}" --mode "${MODE}" --writeraw "${WRITERAW}"
