#!/usr/bin/env bash
set -euo pipefail

PORT=8080

if [ ! -f ./app ]; then
  echo "Binary not found, building first"
  go build -o app .
fi

echo "Starting server on :8080"
exec ./app & APP_PID=$!
for i in {1..10}; do
    if nc -z localhost $PORT; then
        echo "Server is up and running"
        exit 0
    fi
    sleep 1
done

echo "ERROR: Server did not start within 10 seconds."
kill $APP_PID
exit 1
