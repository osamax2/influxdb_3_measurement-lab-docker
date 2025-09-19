#!/bin/sh
set -e

# Performance tuning variables
: ${INFLUXDB_SQLITE_CACHE_SIZE:=4000000000}
: ${INFLUXDB_WAL_BUFFER_SIZE:=536870912}
: ${INFLUXDB_WAL_MAX_BATCH_SIZE:=16777216}
: ${INFLUXDB_WAL_FLUSH_INTERVAL:=2s}
: ${INFLUXDB_QUERY_MEMORY_BYTES:=2147483648}
: ${INFLUXDB_MAX_CONCURRENT_QUERIES:=16}
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
# Map environment tuning to supported CLI flags. Some older names may differ
# between releases; use conservative names reported by the 'serve' help output.
PERF_FLAGS="--wal-max-write-buffer-size ${INFLUXDB_WAL_BUFFER_SIZE} \
            --wal-max-batch-size ${INFLUXDB_WAL_MAX_BATCH_SIZE} \
            --wal-flush-interval ${INFLUXDB_WAL_FLUSH_INTERVAL}"
echo '* soft nofile 131072' >> /etc/security/limits.conf
echo '* hard nofile 262144' >> /etc/security/limits.conf
if [ -w /etc/sysctl.conf ]; then
  echo 'vm.swappiness = 1' >> /etc/sysctl.conf
  echo 'vm.dirty_ratio = 10' >> /etc/sysctl.conf
  echo 'vm.dirty_background_ratio = 5' >> /etc/sysctl.conf
  sysctl -p || true
else
  echo "Skipping sysctl tweaks: /etc/sysctl.conf not writable"
fi
# Skip any host-device mounting inside containerized environments. The compose
# stack provides volumes for $INFLUXDB_DATA_DIR; attempting to mount a device
# like /dev/sdX inside the container will fail and abort startup.
if [ -n "${INFLUXDB_MOUNT_DEVICE:-}" ]; then
  if [ -b "${INFLUXDB_MOUNT_DEVICE}" ]; then
    mount -o noatime,nodiratime,data=writeback,barrier=0,nobh "${INFLUXDB_MOUNT_DEVICE}" "$INFLUXDB_DATA_DIR" || true
  else
    echo "INFLUXDB_MOUNT_DEVICE set but device not present; skipping mount"
  fi
else
  echo "No INFLUXDB_MOUNT_DEVICE configured; skipping device mount"
fi

mkdir -p "$INFLUXDB_DATA_DIR"
chmod 0777 "$INFLUXDB_DATA_DIR" || true

# Persist env-provided runtime token early so local checks/auth can use it.
if [ -n "${INFLUX_TOKEN:-}" ]; then
  if mkdir -p /run >/dev/null 2>&1; then
    # Support raw token (apiv3_...) or a JSON blob containing {"token":"..."}.
    if printf '%s' "${INFLUX_TOKEN}" | sed -n '1s/^[ \t]*\([{\[]\).*/\1/p' | grep -q . >/dev/null 2>&1; then
      # Looks like JSON; write as-is
      printf '%s' "${INFLUX_TOKEN}" > /run/influx_token.tmp || true
    else
      # Raw token -> wrap in documented JSON offline schema
      printf '{"token":"%s","name":"_admin"}' "${INFLUX_TOKEN}" > /run/influx_token.tmp || true
    fi
    chmod 0600 /run/influx_token.tmp || true
    mv /run/influx_token.tmp /run/influx_token || true
    echo "Persisted INFLUX_TOKEN from env to /run/influx_token (0600)"
  fi
fi

# Normalize HTTP bind address early so we pass a valid address to the serve
# command (server rejects bare ":PORT"). Accept :PORT and convert to
# 0.0.0.0:PORT so listen succeeds inside container networks.
if echo "$INFLUXDB_HTTP_BIND_ADDRESS" | grep -q '^:' 2>/dev/null; then
  INFLUXDB_HTTP_BIND_ADDRESS="0.0.0.0${INFLUXDB_HTTP_BIND_ADDRESS}"
fi

ADMIN_TOKEN_FLAG=""
WITHOUT_AUTH_FLAG=""
if [ "${INFLUX_DISABLE_AUTH:-}" = "1" ] || [ "${INFLUX_DISABLE_AUTH:-}" = "true" ]; then
  WITHOUT_AUTH_FLAG="--without-auth"
  echo "Starting server with --without-auth (INFLUX_DISABLE_AUTH=${INFLUX_DISABLE_AUTH})"
fi

# If an admin token file exists at startup (for example mounted from the host
# or written early from the environment), pass it via --admin-token-file so
# the server recognizes the token immediately and doesn't reject client calls.
# If an admin token file exists at startup, detect whether it's JSON and set
# ADMIN_TOKEN_FLAG. If the operator provided INFLUXDB_3_ADMIN_TOKEN we prefer
# to ensure /run/influx_token contains a JSON admin-token file the server can
# parse; overwrite any existing non-JSON token file with the JSON representation
# derived from the env var so the server will accept --admin-token-file.
if [ -f /run/influx_token ]; then
  FIRST_CHAR=$(sed -n '1s/^[ \t]*\([\{\[]\).*/\1/p' /run/influx_token || true)
  if [ "${FIRST_CHAR}" = "{" ] || [ "${FIRST_CHAR}" = "[" ]; then
    ADMIN_TOKEN_FLAG="--admin-token-file /run/influx_token"
    echo "Found JSON admin token file at /run/influx_token; will pass --admin-token-file to server"
  else
    echo "/run/influx_token exists but is not JSON"
    # If operator provided INFLUXDB_3_ADMIN_TOKEN, overwrite the non-JSON file
    # with a JSON admin-token file we control so the server can be started
    # deterministically with --admin-token-file.
    if [ -n "${INFLUXDB_3_ADMIN_TOKEN:-}" ]; then
      echo "Overwriting non-JSON /run/influx_token with JSON admin-token from INFLUXDB_3_ADMIN_TOKEN"
      mkdir -p /run >/dev/null 2>&1 || true
      if [ -n "${INFLUXDB_3_ADMIN_TOKEN_EXPIRY_MILLIS:-}" ]; then
        printf '{"token":"%s","name":"_admin","expiry_millis":%s}' "${INFLUXDB_3_ADMIN_TOKEN}" "${INFLUXDB_3_ADMIN_TOKEN_EXPIRY_MILLIS}" > /run/influx_token.tmp || true
      else
        printf '{"token":"%s","name":"_admin"}' "${INFLUXDB_3_ADMIN_TOKEN}" > /run/influx_token.tmp || true
      fi
      chmod 0600 /run/influx_token.tmp || true
      mv /run/influx_token.tmp /run/influx_token || true
      ADMIN_TOKEN_FLAG="--admin-token-file /run/influx_token"
    else
      echo "Skipping --admin-token-file to avoid parse errors (no INFLUXDB_3_ADMIN_TOKEN provided)"
    fi
  fi
else
  # No token file exists yet. If INFLUXDB_3_ADMIN_TOKEN is present, create a
  # JSON admin-token file so the server can be started with --admin-token-file.
  if [ -n "${INFLUXDB_3_ADMIN_TOKEN:-}" ]; then
    echo "Writing JSON admin token file at /run/influx_token from INFLUXDB_3_ADMIN_TOKEN env"
    mkdir -p /run >/dev/null 2>&1 || true
    if [ -n "${INFLUXDB_3_ADMIN_TOKEN_EXPIRY_MILLIS:-}" ]; then
      printf '{"token":"%s","name":"_admin","expiry_millis":%s}' "${INFLUXDB_3_ADMIN_TOKEN}" "${INFLUXDB_3_ADMIN_TOKEN_EXPIRY_MILLIS}" > /run/influx_token.tmp || true
    else
      printf '{"token":"%s","name":"_admin"}' "${INFLUXDB_3_ADMIN_TOKEN}" > /run/influx_token.tmp || true
    fi
    chmod 0600 /run/influx_token.tmp || true
    mv /run/influx_token.tmp /run/influx_token || true
    ADMIN_TOKEN_FLAG="--admin-token-file /run/influx_token"
  fi
fi

echo "Starting influxdb3 server in background with http-bind=${INFLUXDB_HTTP_BIND_ADDRESS}..."
influxdb3 serve --object-store "$INFLUXDB_OBJECT_STORE" --data-dir "$INFLUXDB_DATA_DIR" --node-id "$INFLUXDB_NODE_ID" --http-bind "$INFLUXDB_HTTP_BIND_ADDRESS" $PERF_FLAGS $ADMIN_TOKEN_FLAG $WITHOUT_AUTH_FLAG &
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

# Only remove the runtime token if we know we will attempt provisioning.
# If the management API is not reachable we'll keep an env-provided token
# persistently so healthchecks and local processes have something to use.
if mkdir -p /run >/dev/null 2>&1; then
  # Don't remove the token here; wait until we detect the management API
  # is reachable before overwriting/removing the env-provided token. This
  # prevents accidental removal of a valid token when the CLI/management
  # API is down or when the server is started with --without-auth.
  :
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

  MAX_ATTEMPTS=6
  ATTEMPT=1
  SECRET=""
  > /run/token_output.txt || true

  # Skip provisioning when server intentionally runs without auth
  if [ -n "${WITHOUT_AUTH_FLAG:-}" ]; then
    echo "Auth disabled for server (WITHOUT_AUTH_FLAG set); skipping token provisioning." >> /run/token_output.txt || true
  else
    # Prefer using an existing apiv3_ token if one exists before attempting to
    # create/regenerate an admin token (which may prompt for confirmation).
    TOKENS_LIST=$(INFLUXDB3_HOST_URL="${INFLUXDB3_HOST_URL}" influxdb3 show tokens 2>&1 || true)
    echo "$TOKENS_LIST" >> /run/token_output.txt || true
    SECRET=$(echo "$TOKENS_LIST" | sed -n 's/.*\(apiv3_[A-Za-z0-9_-]*\).*/\1/p' | head -n1 || true)
    if [ -n "$SECRET" ]; then
      echo "Found existing apiv3 token via CLI: $SECRET" >> /run/token_output.txt || true
      if mkdir -p /run >/dev/null 2>&1; then
        # write documented JSON offline schema atomically
        printf '{"token":"%s","name":"_admin"}' "$SECRET" > /run/influx_token.tmp || true
        chmod 0600 /run/influx_token.tmp || true
        mv /run/influx_token.tmp /run/influx_token || true
      fi
    else
      while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
        echo "Attempt ${ATTEMPT}/${MAX_ATTEMPTS}: creating admin token (non-interactive if possible)..." >> /run/token_output.txt || true
        # Try to use --yes if it exists, otherwise fall back to non-interactive create
        TOKEN_OUTPUT=$(INFLUXDB3_HOST_URL="${INFLUXDB3_HOST_URL}" influxdb3 create token --admin --yes 2>&1 || INFLUXDB3_HOST_URL="${INFLUXDB3_HOST_URL}" influxdb3 create token --admin 2>&1 || true)
        echo "$TOKEN_OUTPUT" >> /run/token_output.txt || true

        # Try to extract apiv3_ token from create output
        SECRET=$(printf "%s" "$TOKEN_OUTPUT" | sed -n 's/.*\(apiv3_[A-Za-z0-9_-]*\).*/\1/p' | head -n1 || true)
        if [ -z "$SECRET" ]; then
          SECRET=$(printf "%s" "$TOKEN_OUTPUT" | sed -n 's/^.*Token: \([^[:space:]]\+\).*$/\1/p' | head -n1 || true)
        fi

        if [ -n "$SECRET" ]; then
          if mkdir -p /run >/dev/null 2>&1; then
            printf '{"token":"%s","name":"_admin"}' "$SECRET" > /run/influx_token.tmp || true
            chmod 0600 /run/influx_token.tmp || true
            mv /run/influx_token.tmp /run/influx_token || true
          fi
          break
        fi

        ATTEMPT=$((ATTEMPT+1))
        sleep 2
      done
    fi
  fi

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
