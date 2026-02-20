#!/usr/bin/env bash
set -euo pipefail

echo "Starting server on :8080"

if [ ! -f ./app ]; then
  echo "Binary not found, building first"
  go build -o app .
fi

exec ./app
