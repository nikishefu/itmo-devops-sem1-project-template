#!/usr/bin/env bash
set -euo pipefail

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=project-sem-1
POSTGRES_USER=validator
POSTGRES_PASSWORD=val1dat0r

go mod tidy
go build -o app .

echo "Preparing database schema"

if command -v psql >/dev/null 2>&1; then
  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$POSTGRES_HOST" \
    -p "$POSTGRES_PORT" \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -v ON_ERROR_STOP=1 \
    -c "CREATE TABLE IF NOT EXISTS prices (
          id SERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          category VARCHAR(255) NOT NULL,
          price DECIMAL(10,2) NOT NULL,
          create_date TIMESTAMP NOT NULL
        );"
  echo "Database ready"
else
  echo "psql not found, schema will be created automatically on app start"
fi

echo "Prepare completed"
