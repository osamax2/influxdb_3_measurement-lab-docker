ARG BASE_IMAGE=influxdb:3-core
FROM ${BASE_IMAGE}

# Install performance monitoring tools if apt-get exists (some base images may be minimal)
RUN if [ "$(id -u)" = "0" ] && command -v apt-get >/dev/null 2>&1; then \
            mkdir -p /var/lib/apt/lists/partial && \
            apt-get update && apt-get install -y \
                htop \
                iotop \
                sysstat \
                && rm -rf /var/lib/apt/lists/* ; \
        else \
            echo "Not running as root or apt-get not found, skipping performance tool installation"; \
        fi

# Copy init script and set executable bit
COPY --chmod=0755 docker-entrypoint-init.sh /usr/local/bin/docker-entrypoint-init.sh
COPY --chmod=0755 healthcheck.sh /usr/local/bin/healthcheck.sh

# Normalize line endings
RUN sed -i 's/\r$//' /usr/local/bin/docker-entrypoint-init.sh \
 && sed -i 's/\r$//' /usr/local/bin/healthcheck.sh || true

# Additional normalization
RUN mkdir -p /usr/local/bin || true && \
    (tr -d '\r' < /usr/local/bin/docker-entrypoint-init.sh > /usr/local/bin/docker-entrypoint-init.sh.fixed && mv /usr/local/bin/docker-entrypoint-init.sh.fixed /usr/local/bin/docker-entrypoint-init.sh) || true && \
    (tr -d '\r' < /usr/local/bin/healthcheck.sh > /usr/local/bin/healthcheck.sh.fixed && mv /usr/local/bin/healthcheck.sh.fixed /usr/local/bin/healthcheck.sh) || true

# Performance tuning sysctls (applied at runtime)
RUN if [ "$(id -u)" = "0" ]; then \
            echo 'vm.swappiness = 1' >> /etc/sysctl.conf && \
            echo 'vm.dirty_ratio = 10' >> /etc/sysctl.conf && \
            echo 'vm.dirty_background_ratio = 5' >> /etc/sysctl.conf; \
        else \
            echo "/etc/sysctl.conf not writable, skipping sysctl defaults"; \
        fi

ENTRYPOINT ["sh", "-c", "sysctl -p; sed -i 's/\\r$//' /usr/local/bin/docker-entrypoint-init.sh || true; sed -i 's/\\r$//' /usr/local/bin/healthcheck.sh || true; exec sh -e /usr/local/bin/docker-entrypoint-init.sh"]