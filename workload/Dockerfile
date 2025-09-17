# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build

WORKDIR /src
COPY Workload.csproj ./
RUN dotnet restore

COPY src/ ./src/
COPY config/ ./config/
RUN dotnet publish -c Release -o /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:8.0
# Propagate proxy variables to runtime if they were provided at build time
WORKDIR /app
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

# Install curl for health probes (small, commonly available)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

# Create config directory
RUN mkdir -p /app/config

# Create non-root user
ARG USER=worker
ARG UID=1000
ARG GID=1000
RUN groupadd -g ${GID} ${USER} && \
    useradd -u ${UID} -g ${GID} -m ${USER} && \
    mkdir -p /logs/workload && \
    chown ${USER}:${USER} /logs/workload

# Copy application files
COPY --from=build /app/publish/ ./
COPY config/ /app/config/

# Copy and prepare entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && \
    chmod 755 /entrypoint.sh && \
    chown ${USER}:${USER} /entrypoint.sh && \
    chown -R ${UID}:${GID} /app

USER ${USER}
ENTRYPOINT ["/entrypoint.sh"]