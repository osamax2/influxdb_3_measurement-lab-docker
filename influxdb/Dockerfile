ARG BASE_IMAGE=influxdb:3-core
FROM ${BASE_IMAGE}

# Copy init script and set executable bit in a single step (avoids permission issues on some hosts)
COPY --chmod=0755 docker-entrypoint-init.sh /usr/local/bin/docker-entrypoint-init.sh
COPY --chmod=0755 healthcheck.sh /usr/local/bin/healthcheck.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint-init.sh"]
