ARG BASE_IMAGE=influxdb:3-core
FROM ${BASE_IMAGE}

# Copy init script and set executable bit in a single step (avoids permission issues on some hosts)
COPY --chmod=0755 docker-entrypoint-init.sh /usr/local/bin/docker-entrypoint-init.sh
COPY --chmod=0755 healthcheck.sh /usr/local/bin/healthcheck.sh

# Normalize line endings in the copied scripts to avoid CRLF-related exec errors
RUN sed -i 's/\r$//' /usr/local/bin/docker-entrypoint-init.sh \
 && sed -i 's/\r$//' /usr/local/bin/healthcheck.sh || true

# As an extra normalization step, use tr to remove any stray CR characters.
RUN mkdir -p /usr/local/bin || true && \
	# Defensive normalization of scripts copied into the image (remove stray CRs)
	(tr -d '\r' < /usr/local/bin/docker-entrypoint-init.sh > /usr/local/bin/docker-entrypoint-init.sh.fixed && mv /usr/local/bin/docker-entrypoint-init.sh.fixed /usr/local/bin/docker-entrypoint-init.sh) || true && \
	(tr -d '\r' < /usr/local/bin/healthcheck.sh > /usr/local/bin/healthcheck.sh.fixed && mv /usr/local/bin/healthcheck.sh.fixed /usr/local/bin/healthcheck.sh) || true

# At container start, normalize CRLF -> LF on the init script and then exec it
# with sh -e. Doing this at runtime guarantees the script will run even if the
# file was checked out with CRLF on the host.
ENTRYPOINT ["sh", "-c", "sed -i 's/\\r$//' /usr/local/bin/docker-entrypoint-init.sh || true; sed -i 's/\\r$//' /usr/local/bin/healthcheck.sh || true; exec sh -e /usr/local/bin/docker-entrypoint-init.sh"]
