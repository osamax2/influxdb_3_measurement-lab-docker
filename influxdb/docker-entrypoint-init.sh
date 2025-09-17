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
# Ensure the server binds its HTTP endpoint to the configured address.
# If the bind address is of the form ':PORT', prefix with 0.0.0.0 so it's a valid socket address.
if echo "$INFLUXDB_HTTP_BIND_ADDRESS" | grep -q '^:' 2>/dev/null; then
  INFLUXDB_HTTP_BIND_ADDRESS="0.0.0.0${INFLUXDB_HTTP_BIND_ADDRESS}"
fi

# NOTE: Writing a plain token to an admin-token-file path causes the server to
# expect JSON and fail parsing. To avoid brittle startup failures across server
# versions, do NOT pass an admin-token-file at startup. Instead, the init script
# will create an admin token after the server reports healthy and persist the
# secret to /run/influx_token for consumers.
ADMIN_TOKEN_FLAG=""

# Optionally run server without authorization (makes testing easier). Set
# INFLUX_DISABLE_AUTH=1 in compose/.env to enable.
WITHOUT_AUTH_FLAG=""
if [ "${INFLUX_DISABLE_AUTH:-}" = "1" ] || [ "${INFLUX_DISABLE_AUTH:-}" = "true" ]; then
  WITHOUT_AUTH_FLAG="--without-auth"
  echo "Starting server with --without-auth (INFLUX_DISABLE_AUTH=${INFLUX_DISABLE_AUTH})"
fi

influxdb3 serve --object-store "$INFLUXDB_OBJECT_STORE" --data-dir "$INFLUXDB_DATA_DIR" --node-id "$INFLUXDB_NODE_ID" --http-bind "$INFLUXDB_HTTP_BIND_ADDRESS" $ADMIN_TOKEN_FLAG $WITHOUT_AUTH_FLAG &
INFLUXD_PID=$!

echo "Waiting for InfluxDB to become ready (HTTP health)..."
RETRIES=60
# If the bind address was provided as ':PORT' or '0.0.0.0:PORT', extract the
# port component for the health probe. Default to 8086.
INFLUXDB_HTTP_BIND_PORT=8086
# Derive the port from INFLUXDB_HTTP_BIND_ADDRESS in a POSIX-safe way. Handles
# values like ':8086', '0.0.0.0:8086' or '127.0.0.1:8086'. Fall back to 8086.
if echo "$INFLUXDB_HTTP_BIND_ADDRESS" | grep -q ':' 2>/dev/null; then
  # parameter expansion to extract text after the last ':'
  INFLUXDB_HTTP_BIND_PORT="${INFLUXDB_HTTP_BIND_ADDRESS##*:}"
  # sanity-check: keep only digits
  INFLUXDB_HTTP_BIND_PORT=$(echo "$INFLUXDB_HTTP_BIND_PORT" | sed 's/[^0-9]*//g' || true)
  if [ -z "$INFLUXDB_HTTP_BIND_PORT" ]; then
    INFLUXDB_HTTP_BIND_PORT=8086
  fi
fi
HEALTH_URL="http://127.0.0.1:${INFLUXDB_HTTP_BIND_PORT}/health"
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
# Run CLI against the server HTTP bind address so the CLI talks to the right port.
CLI_HOST="http://127.0.0.1:${INFLUXDB_HTTP_BIND_PORT}"

if ! influxdb3 show databases 2>/dev/null | grep -q "^$INFLUXDB_BUCKET$"; then
  influxdb3 create database "$INFLUXDB_BUCKET" >/dev/null 2>&1 || true
fi

echo "Creating admin token (ensure a server-registered token is written to /run/influx_token)"

# Remove any stale runtime token file before creating a new one. We will overwrite
# /run/influx_token with the token the server actually registers.
if mkdir -p /run >/dev/null 2>&1; then
  if [ -f /run/influx_token ]; then
    echo "Removing stale /run/influx_token"
    rm -f /run/influx_token || true
  fi
fi

# Wait for InfluxDB management API (CLI control) port to accept TCP connections.
# The CLI talks to the control plane on port 8181 by default; wait for that port
# to be reachable before attempting token creation to avoid connection refused.
# Use the same HTTP bind port for management API as the server HTTP bind port.
# The server may have relocated the management API when --http-bind is overridden.
# Possible management API ports to try. Historically the CLI/control plane
# listens on 8181, but some server builds expose management on the HTTP bind
# port. Try 8181 first, then the HTTP bind port.
MGMT_PORT_CANDIDATES="8181 ${INFLUXDB_HTTP_BIND_PORT}"
# Give the management API some time to come up; it can lag behind the main
# HTTP bind. Wait up to 120s in total for any candidate to accept TCP connects.
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
  echo "ERROR: Management API not reachable on any candidate ports (${MGMT_PORT_CANDIDATES}); aborting token creation" >&2
  kill $INFLUXD_PID 2>/dev/null || true
  exit 1
fi

# Export the address the CLI should target so helper commands use the
# management API we discovered above. The CLI looks for INFLUXDB3_HOST_URL
# (and some subcommands read INFLUXDB3_HOST_URL / INFLUXDB3_HOST_URL env), so
# set that to the discovered management address. This ensures `influxdb3`
# client uses the correct host instead of defaulting to 127.0.0.1:8181.
export INFLUXDB3_HOST_URL="http://127.0.0.1:${MGMT_PORT}"

# Try to create an admin token via the CLI with retries. Token creation may fail
# transiently while the control plane finishes initialization; retry several
# times with a short backoff before giving up. Capture CLI output to /run/token_output.txt
# so failures are visible on the host.
MAX_ATTEMPTS=12
ATTEMPT=1
SECRET=""
> /run/token_output.txt || true
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
  echo "Attempt ${ATTEMPT}/${MAX_ATTEMPTS}: creating admin token..." >> /run/token_output.txt || true
  # Invoke the CLI with an explicit host so it doesn't default to 127.0.0.1:8181
  if [ -n "${INFLUXDB3_HOST_URL:-}" ]; then
    # Provide automatic confirmation in case the CLI prompts to regenerate
    # an existing admin token. This avoids hanging or interactive failures
    # during container init.
    TOKEN_OUTPUT=$(printf 'yes\n' | influxdb3 -H "${INFLUXDB3_HOST_URL}" create token --admin 2>&1 || true)
  else
    TOKEN_OUTPUT=$(printf 'yes\n' | influxdb3 create token --admin 2>&1 || true)
  fi
  echo "$TOKEN_OUTPUT" >> /run/token_output.txt || true

  # Strategy 1: look for an explicit apiv3_ secret in command output
  SECRET=$(echo "$TOKEN_OUTPUT" | sed -n 's/.*\(apiv3_[A-Za-z0-9_-]*\).*/\1/p' | head -n1 || true)

  # Strategy 2: look for lines like 'Token: <secret>'
  if [ -z "$SECRET" ]; then
    SECRET=$(echo "$TOKEN_OUTPUT" | sed -n 's/^.*Token: \([^[:space:]]\+\).*$/\1/p' | head -n1 || true)
  fi

  if [ -n "$SECRET" ]; then
    # Persist token to runtime file (overwrite any previous file)
    if mkdir -p /run >/dev/null 2>&1; then
      printf "%s" "$SECRET" > /run/influx_token || true
      chmod 0644 /run/influx_token || true
    fi
    # Optionally inject into a host env file if requested
    if [ -n "$INJECT_TOKEN_TO_ENV_PATH" ] && [ -w "$INJECT_TOKEN_TO_ENV_PATH" ]; then
      grep -v '^INFLUX_TOKEN=' "$INJECT_TOKEN_TO_ENV_PATH" > "$INJECT_TOKEN_TO_ENV_PATH.tmp" || true
      printf "%s\n" "INFLUX_TOKEN=$SECRET" >> "$INJECT_TOKEN_TO_ENV_PATH.tmp"
      mv "$INJECT_TOKEN_TO_ENV_PATH.tmp" "$INJECT_TOKEN_TO_ENV_PATH"
    fi
    break
    fi

    # If we didn't get a secret, optionally try listing tokens (best-effort) and retry.
    if [ -n "${INFLUXDB3_HOST_URL:-}" ]; then
      TOKENS_LIST=$(influxdb3 -H "${INFLUXDB3_HOST_URL}" show tokens 2>&1 || true)
    else
      TOKENS_LIST=$(influxdb3 show tokens 2>&1 || true)
    fi
    echo "$TOKENS_LIST" >> /run/token_output.txt || true
    SECRET=$(echo "$TOKENS_LIST" | sed -n 's/.*\(apiv3_[A-Za-z0-9_-]*\).*/\1/p' | head -n1 || true)
    if [ -n "$SECRET" ]; then
      echo "Token discovered from token list: $SECRET" >> /run/token_output.txt || true
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
    echo "InfluxDB token provisioning failed; terminating." >&2
    kill $INFLUXD_PID 2>/dev/null || true
    exit 1
  fi

  echo "Initialization complete. Bringing foreground process..."
  wait $INFLUXD_PID
