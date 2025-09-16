#!/bin/sh
set -e

# Default environment variables (can be overridden by docker-compose or docker run -e)
: ${INFLUXDB_HTTP_BIND_ADDRESS:=:8086}
: ${INFLUXDB_3_ADMIN_TOKEN:=admin-token}
: ${INFLUXDB_ORG:=my-org}
: ${INFLUXDB_BUCKET:=my-bucket}
: ${INFLUXDB_RETENTION:=0}
: ${INFLUXDB_ADMIN_USER:=admin}
: ${INFLUXDB_ADMIN_PASSWORD:=password}
: ${INFLUXDB_NODE_ID:=node1}
: ${INFLUXDB_OBJECT_STORE:=file}
: ${INFLUXDB_DATA_DIR:=/var/lib/influxdb}

echo "Preparing data dir: $INFLUXDB_DATA_DIR"
mkdir -p "$INFLUXDB_DATA_DIR"
# Try to make data dir writable; avoid chown (user may not exist in image). Use
# permissive permissions so the server can write regardless of runtime user.
chmod 0777 "$INFLUXDB_DATA_DIR" || true

# If the operator provided an INFLUX_TOKEN via environment (e.g. from root .env),
# persist it to /run/influx_token so the container-local healthcheck and local
# consumers can authenticate immediately. This is idempotent and safe to run
# on repeated container restarts.
if [ -n "${INFLUX_TOKEN:-}" ]; then
  if mkdir -p /run >/dev/null 2>&1; then
    printf "%s" "${INFLUX_TOKEN}" > /run/influx_token || true
    chmod 0644 /run/influx_token || true
    echo "Persisted INFLUX_TOKEN from env to /run/influx_token"
  fi
fi

echo "Starting influxdb3 server in background..."
influxdb3 serve --object-store "$INFLUXDB_OBJECT_STORE" --data-dir "$INFLUXDB_DATA_DIR" --node-id "$INFLUXDB_NODE_ID" &
INFLUXD_PID=$!

echo "Waiting for InfluxDB to become ready (HTTP health on port 8181)..."
RETRIES=60
HEALTH_URL="http://127.0.0.1:8181/health"
while [ $RETRIES -gt 0 ]; do
  # Accept any HTTP response code (401 indicates server up but requiring auth).
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || echo "000")
  if [ "$HTTP_CODE" != "000" ]; then
    echo "InfluxDB HTTP responded with status $HTTP_CODE"
    break
  fi
  # Fallback to CLI check if available and unauthenticated checks succeed
  if influxdb3 show system >/dev/null 2>&1; then
    echo "InfluxDB CLI reports system up"
    break
  fi
  sleep 1
  RETRIES=$((RETRIES-1))
done

if [ $RETRIES -le 0 ]; then
  echo "InfluxDB did not start in time"
  kill $INFLUXD_PID || true
  exit 1
fi

echo "Creating database (if missing): $INFLUXDB_BUCKET"
# Use positional database name (the 3-core CLI expects the DB name as the final arg).
# Avoid passing flags that older/newer builds of the CLI may not accept.
if ! influxdb3 show databases 2>/dev/null | grep -q "^$INFLUXDB_BUCKET$"; then
  influxdb3 create database "$INFLUXDB_BUCKET" >/dev/null 2>&1 || true
fi

echo "Creating admin token (or ensure one exists)"
# The 3-core CLI exposes 'create token --admin' to create/regenerate an admin token.
# Create an admin token (server will assign the secret) and then list tokens to extract a token
# that has wildcard permissions (admin). This avoids relying on CLI to print the secret directly.
influxdb3 create token --admin >/dev/null 2>&1 || true

# Capture the admin token by finding a token row that contains wildcard permissions '*:*:*'
TOKENS_LIST=$(influxdb3 show tokens 2>/dev/null || true)
# The token id/hex is in the first column; try to find the token id for the admin token row then
# attempt to find a secret-like string (apiv3_...) in the tokens output.
GENERATED_TOKEN=$(echo "$TOKENS_LIST" | awk '/\*:\*:\*/{print $1; exit}' || true)
if [ -n "$GENERATED_TOKEN" ]; then
  # If the tokens listing printed token ids, try to map that id to a secret by searching for apiv3_ in the listing.
  SECRET=$(echo "$TOKENS_LIST" | sed -n 's/.*\(apiv3_[[:alnum:]_-]*\).*/\1/p' | head -n1 || true)
  if [ -n "$SECRET" ]; then
    GENERATED_TOKEN="$SECRET"
  fi
fi
if [ -z "$GENERATED_TOKEN" ]; then
  # As a final fallback, attempt to create a token and parse any 'Token:' lines from stdout.
  TOKEN_OUTPUT=$(influxdb3 create token 2>/dev/null || true)
  GENERATED_TOKEN=$(echo "$TOKEN_OUTPUT" | sed -n 's/^.*Token: \([^[:space:]]\+\).*$/\1/p' | head -n1 || true)
fi

# If we obtained a token and a writable host .env path is provided via ENV_INJECT_PATH, append it there for consumer services.
if [ -n "$GENERATED_TOKEN" ]; then
  echo "Token created: ${GENERATED_TOKEN}"
  # Persist token to a runtime file readable inside the container for healthchecks.
  if [ -w /run ] || mkdir -p /run >/dev/null 2>&1; then
    printf "%s" "${GENERATED_TOKEN}" > /run/influx_token || true
    chmod 0644 /run/influx_token || true
  fi
  if [ -n "$INJECT_TOKEN_TO_ENV_PATH" ] && [ -w "$INJECT_TOKEN_TO_ENV_PATH" ]; then
    # Avoid writing duplicate entries - remove any existing INFLUX_TOKEN line then append
    grep -v '^INFLUX_TOKEN=' "$INJECT_TOKEN_TO_ENV_PATH" > "$INJECT_TOKEN_TO_ENV_PATH.tmp" || true
    printf "%s\n" "INFLUX_TOKEN=${GENERATED_TOKEN}" >> "$INJECT_TOKEN_TO_ENV_PATH.tmp"
    mv "$INJECT_TOKEN_TO_ENV_PATH.tmp" "$INJECT_TOKEN_TO_ENV_PATH"
  fi
fi

echo "Initialization complete. Bringing foreground process..."
wait $INFLUXD_PID
    GENERATED_TOKEN=$(echo "$TOKEN_OUTPUT" | sed -n 's/^.*Token: \([^[:space:]]\+\).*$/\1/p' | head -n1 || true)
