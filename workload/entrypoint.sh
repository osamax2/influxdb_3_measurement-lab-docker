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

# Execute workload
# Unset proxy variables so local traffic to the influxdb container is not routed via host proxy
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy NO_PROXY no_proxy

echo "[entrypoint.sh] Running: dotnet /app/Workload.dll $@" | tee -a "$LOGFILE"
echo "[entrypoint.sh] Listing /logs/workload before workload execution:" | tee -a "$LOGFILE"
ls -l /logs/workload | tee -a "$LOGFILE"

exec dotnet /app/Workload.dll "$@" > "$OUTFILE" 2>&1