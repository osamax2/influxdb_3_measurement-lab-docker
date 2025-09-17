#!/bin/sh
set -e

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
chmod 0777 "$INFLUXDB_DATA_DIR" || true

# Persist env-provided runtime token early so local checks/auth can use it.
if [ -n "${INFLUX_TOKEN:-}" ]; then
  if mkdir -p /run >/dev/null 2>&1; then
    printf "%s" "${INFLUX_TOKEN}" > /run/influx_token || true
    chmod 0644 /run/influx_token || true
    echo "Persisted INFLUX_TOKEN from env to /run/influx_token"
  fi
fi

echo "Starting influxdb3 server in background..."
if echo "$INFLUXDB_HTTP_BIND_ADDRESS" | grep -q '^:' 2>/dev/null; then
  INFLUXDB_HTTP_BIND_ADDRESS="0.0.0.0${INFLUXDB_HTTP_BIND_ADDRESS}"
fi

ADMIN_TOKEN_FLAG=""
WITHOUT_AUTH_FLAG=""
if [ "${INFLUX_DISABLE_AUTH:-}" = "1" ] || [ "${INFLUX_DISABLE_AUTH:-}" = "true" ]; then
  WITHOUT_AUTH_FLAG="--without-auth"
  echo "Starting server with --without-auth (INFLUX_DISABLE_AUTH=${INFLUX_DISABLE_AUTH})"
fi

influxdb3 serve --object-store "$INFLUXDB_OBJECT_STORE" --data-dir "$INFLUXDB_DATA_DIR" --node-id "$INFLUXDB_NODE_ID" --http-bind "$INFLUXDB_HTTP_BIND_ADDRESS" $ADMIN_TOKEN_FLAG $WITHOUT_AUTH_FLAG &
INFLUXD_PID=$!

echo "Waiting for InfluxDB to become ready (HTTP health)..."
RETRIES=60
INFLUXDB_HTTP_BIND_PORT=8086
if echo "$INFLUXDB_HTTP_BIND_ADDRESS" | grep -q ':' 2>/dev/null; then
  INFLUXDB_HTTP_BIND_PORT="${INFLUXDB_HTTP_BIND_ADDRESS##*:}"
  INFLUXDB_HTTP_BIND_PORT=$(echo "$INFLUXDB_HTTP_BIND_PORT" | sed 's/[^0-9]*//g' || true)
  if [ -z "$INFLUXDB_HTTP_BIND_PORT" ]; then
    INFLUXDB_HTTP_BIND_PORT=8086
  fi
fi
HEALTH_URL="http://127.0.0.1:${INFLUXDB_HTTP_BIND_PORT}/health"
while [ $RETRIES -gt 0 ]; do
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || echo "000")
  if [ "$HTTP_CODE" != "000" ]; then
    echo "InfluxDB HTTP responded with status $HTTP_CODE"
    break
  fi
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
CLI_HOST="http://127.0.0.1:${INFLUXDB_HTTP_BIND_PORT}"
if ! influxdb3 show databases 2>/dev/null | grep -q "^$INFLUXDB_BUCKET$"; then
  influxdb3 create database "$INFLUXDB_BUCKET" >/dev/null 2>&1 || true
fi

echo "Creating admin token (ensure a server-registered token is written to /run/influx_token)"

 # Always attempt CLI-based token provisioning so the server registers a valid
 # apiv3 token. If the operator supplied `INFLUX_TOKEN` we write it early for
 # local healthchecks, but that value is not sufficient to register auth with
 # the server. The CLI / management API will generate a server-known token and
 # we persist that to `/run/influx_token`, overwriting the env value if present.

# Remove any stale runtime token file before creating a new one.
if mkdir -p /run >/dev/null 2>&1; then
  if [ -f /run/influx_token ]; then
    echo "Removing stale /run/influx_token"
    rm -f /run/influx_token || true
  fi
fi

MGMT_PORT_CANDIDATES="8181 ${INFLUXDB_HTTP_BIND_PORT}"
MGMT_RETRIES=120
echo "Waiting for management API on one of: ${MGMT_PORT_CANDIDATES} (up to ${MGMT_RETRIES}s)"
while [ $MGMT_RETRIES -gt 0 ]; do
  for P in ${MGMT_PORT_CANDIDATES}; do
    if (echo > /dev/tcp/127.0.0.1/${P}) >/dev/null 2>&1; then
      MGMT_PORT=${P}
      echo "Management API is reachable on 127.0.0.1:${MGMT_PORT}"
      break 2
    fi
  done
  sleep 1
  MGMT_RETRIES=$((MGMT_RETRIES-1))
done
if [ -z "${MGMT_PORT:-}" ]; then
  echo "WARNING: Management API not reachable on any candidate ports (${MGMT_PORT_CANDIDATES}); token creation will be skipped and the env-provided token (if any) will be used for local checks only." >&2
else
  export INFLUXDB3_HOST_URL="http://127.0.0.1:${MGMT_PORT}"

  MAX_ATTEMPTS=12
  ATTEMPT=1
  SECRET=""
  > /run/token_output.txt || true
  while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    echo "Attempt ${ATTEMPT}/${MAX_ATTEMPTS}: creating admin token..." >> /run/token_output.txt || true
    TOKEN_OUTPUT=$(printf 'yes\n' | influxdb3 -H "${INFLUXDB3_HOST_URL}" create token --admin 2>&1 || true)
    echo "$TOKEN_OUTPUT" >> /run/token_output.txt || true

    SECRET=$(echo "$TOKEN_OUTPUT" | sed -n 's/.*\(apiv3_[A-Za-z0-9_-]*\).*/\1/p' | head -n1 || true)
    if [ -z "$SECRET" ]; then
      SECRET=$(echo "$TOKEN_OUTPUT" | sed -n 's/^.*Token: \([^[:space:]]\+\).*$/\1/p' | head -n1 || true)
    fi

    if [ -n "$SECRET" ]; then
      if mkdir -p /run >/dev/null 2>&1; then
        printf "%s" "$SECRET" > /run/influx_token || true
        chmod 0644 /run/influx_token || true
      fi
      break
    fi

    TOKENS_LIST=$(influxdb3 -H "${INFLUXDB3_HOST_URL}" show tokens 2>&1 || true)
    echo "$TOKENS_LIST" >> /run/token_output.txt || true
    SECRET=$(echo "$TOKENS_LIST" | sed -n 's/.*\(apiv3_[A-Za-z0-9_-]*\).*/\1/p' | head -n1 || true)
    if [ -n "$SECRET" ]; then
      if mkdir -p /run >/dev/null 2>&1; then
        printf "%s" "$SECRET" > /run/influx_token || true
        chmod 0644 /run/influx_token || true
      fi
      break
    fi

    ATTEMPT=$((ATTEMPT+1))
    sleep 2
  done

  if [ -z "$SECRET" ]; then
    echo "ERROR: Failed to obtain admin token from CLI after ${MAX_ATTEMPTS} attempts. See /run/token_output.txt for details." >&2
    echo "--- final create output ---" >> /run/token_output.txt || true
    echo "$TOKEN_OUTPUT" >> /run/token_output.txt || true
    echo "--- final show tokens output ---" >> /run/token_output.txt || true
    echo "$TOKENS_LIST" >> /run/token_output.txt || true
    echo "InfluxDB token provisioning failed; continuing with env-provided token (if any)." >&2
  fi
fi

echo "Initialization complete. Bringing foreground process..."
wait $INFLUXD_PID
  # Invoke the CLI with an explicit host so it doesn't default to 127.0.0.1:8181
