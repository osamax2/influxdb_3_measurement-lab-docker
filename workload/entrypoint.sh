#!/usr/bin/env bash
set -euo pipefail

# Create log directory
mkdir -p /logs/workload
LOGFILE="/logs/workload/entrypoint_debug.log"
OUTFILE="/logs/workload/workload_stdout.log"

# Log startup
echo "[entrypoint.sh] Starting Workload container at $(date)" | tee "$LOGFILE"
ls -l /app/Workload.dll | tee -a "$LOGFILE"

# Validate numeric environment variables
validate_numeric() {
  local var_name=$1
  local var_value=${!var_name:-}
  
  if [[ -n "$var_value" ]]; then
    if [[ "$var_value" =~ ^[0-9]+$ ]]; then
      echo "[entrypoint.sh] Valid $var_name: $var_value" | tee -a "$LOGFILE"
    else
      echo "[entrypoint.sh] WARNING: Invalid $var_name value '$var_value', defaulting to empty" | tee -a "$LOGFILE"
      export "${var_name}="
    fi
  fi
}

# Validate all numeric variables
validate_numeric DURATION_SEC
validate_numeric BATCH_SIZE
validate_numeric INFLUX_PORT
validate_numeric POINTS
validate_numeric TIMESTAMP_SPAN_SEC
validate_numeric PARALLEL_CLIENTS
validate_numeric SERIES_MULTIPLIER

# Log environment
echo "[entrypoint.sh] ENV VARS:" | tee -a "$LOGFILE"
env | grep -E 'SCENARIO|REPORT|WORKLOAD_LOG_DIR|INFLUX|MEASUREMENTS|TAGS|POINTS|TIMESTAMP_SPAN_SEC|PARALLEL_CLIENTS|SERIES_MULTIPLIER|BATCH_SIZE|DURATION_SEC' | tee -a "$LOGFILE"

# Wait for InfluxDB HTTP health endpoint before starting workload to avoid early
# connection-refused errors. This checks localhost:8181 inside the influxdb
# container via the service name and waits up to 60s.

# Determine health endpoint and wait for InfluxDB to respond before starting
# the workload. Waiting first ensures any server-side token creation has time
# to complete so we can prefer a runtime token file if present.
# Use the configured Influx write port for health checks so we probe the
# same HTTP endpoint the server binds. Default to 8086 when INFLUX_PORT is
# not provided.
: ${INFLUX_PORT:=8086}
: ${INFLUX_HOST:=influxdb}
HEALTH_URL="http://${INFLUX_HOST}:${INFLUX_PORT}/health"
RETRIES=60
echo "[entrypoint.sh] Waiting for InfluxDB at $HEALTH_URL (up to $RETRIES s)" | tee -a "$LOGFILE"
while [ $RETRIES -gt 0 ]; do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || echo "000")
  # Only accept a real 3-digit HTTP code that is not '000'. Some health helpers
  # return '000' as a sentinel; ignore those and continue waiting.
  if echo "$CODE" | grep -qE '^[0-9]{3}$' && [ "$CODE" != "000" ]; then
    echo "[entrypoint.sh] InfluxDB health responded: $CODE" | tee -a "$LOGFILE"
    break
  fi
  sleep 1
  RETRIES=$((RETRIES-1))
done

# After the server reports a health response, prefer a runtime token created by
# the server at /run/influx_token if available. This avoids a race where the
# workload reads the token too early and uses an out-of-date value from .env.
if [ -r /run/influx_token ]; then
  TOK=$(cat /run/influx_token 2>/dev/null || true)
  if [ -n "$TOK" ]; then
    export INFLUX_TOKEN="$TOK"
    echo "[entrypoint.sh] Loaded INFLUX_TOKEN from /run/influx_token" | tee -a "$LOGFILE"
  fi
fi

# Additionally wait for the InfluxDB TCP port to accept connections to avoid
# transient Connection refused errors when HTTP is up but the write endpoint
# isn't yet listening. This uses bash /dev/tcp and will wait up to 60s.
TCP_RETRIES=60
INFLUX_P=${INFLUX_PORT:-8086}
INFLUX_H=${INFLUX_HOST:-influxdb}
echo "[entrypoint.sh] Waiting for TCP ${INFLUX_H}:${INFLUX_P} (up to $TCP_RETRIES s)" | tee -a "$LOGFILE"
while [ $TCP_RETRIES -gt 0 ]; do
  if (echo > /dev/tcp/${INFLUX_H}/${INFLUX_P}) >/dev/null 2>&1; then
    echo "[entrypoint.sh] TCP ${INFLUX_H}:${INFLUX_P} is reachable" | tee -a "$LOGFILE"
    break
  fi
  sleep 1
  TCP_RETRIES=$((TCP_RETRIES-1))
done
if [ $TCP_RETRIES -le 0 ]; then
  echo "[entrypoint.sh] ERROR: Timeout waiting for TCP ${INFLUX_H}:${INFLUX_P}" | tee -a "$LOGFILE"
  exit 1
fi

# Execute workload
echo "[entrypoint.sh] Running: dotnet /app/Workload.dll $@" | tee -a "$LOGFILE"
echo "[entrypoint.sh] Listing /logs/workload before workload execution:" | tee -a "$LOGFILE"
ls -l /logs/workload | tee -a "$LOGFILE"

exec dotnet /app/Workload.dll "$@" > "$OUTFILE" 2>&1