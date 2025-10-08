#!/usr/bin/env bash
set -euo pipefail
set -x
export HISTIGNORE='*sudo -S*'
echo "tesla12459" | sudo -S -k chown -R 1000:1000 logs/workload
mkdir -p /Users/osamaalabaji/influxdb_3_measurement-lab-docker/workload/host_logs_run1 && docker compose run --rm -v /Users/osamaalabaji/influxdb_3_measurement-lab-docker/workload/host_logs_run1:/logs/workload -e POINTS=10000000 -e PARALLEL_CLIENTS=16 -e POINTS_PER_REQUEST=5000 -e SEND_CONCURRENCY=32 -e USE_GZIP=true -e USE_V3_WRITE_LP=true -e V3_NO_SYNC=true -e V3_ACCEPT_PARTIAL=true workload
