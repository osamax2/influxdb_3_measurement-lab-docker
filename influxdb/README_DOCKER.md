InfluxDB 3 Docker setup

This folder contains a simple Dockerfile, entrypoint script, and docker-compose file to run InfluxDB 3 and perform initial setup (create org, bucket, admin user and token).

Files added:
- `Dockerfile` - builds from official `influxdb:3-core` image and adds init script
- `docker-entrypoint-init.sh` - starts `influxd`, waits until it's ready and runs `influx setup` if needed
- `docker-compose.yml` - example compose to build and run the service

Quick start

1. Build and start with docker-compose:

```sh
docker compose up --build
```

2. Verify the bucket was created:

```sh
docker compose exec influxdb influx bucket list -o my-org
```

Environment variables
- `INFLUXDB_3_ADMIN_TOKEN` - token to set for admin (default: admin-token)
- `INFLUXDB_ORG` - organization name (default: my-org)
- `INFLUXDB_BUCKET` - bucket name (default: my-bucket)
- `INFLUXDB_ADMIN_USER` - admin username (default: admin)
- `INFLUXDB_ADMIN_PASSWORD` - admin password (default: password)

Notes and next steps
- The init script uses `influx setup --force` to ensure idempotent setup. Adjust retention and other settings as needed.
- For production, change passwords/tokens and enable secure storage of secrets.
InfluxDB 3 Docker setup

This folder contains a simple Dockerfile, entrypoint script, and docker-compose file to run InfluxDB 3 and perform initial setup (create org, bucket, admin user and token).

Files added:
- `Dockerfile` - builds from official `influxdb:3-core` image and adds init script
- `docker-entrypoint-init.sh` - starts `influxd`, waits until it's ready and runs `influx setup` if needed
- `docker-compose.yml` - example compose to build and run the service

Quick start

1. Build and start with docker-compose:

```sh
docker compose up --build
```

2. Verify the bucket was created:

```sh
docker compose exec influxdb influx bucket list -o my-org
```

Environment variables
- `INFLUXDB_3_ADMIN_TOKEN` - token to set for admin (default: admin-token)
- `INFLUXDB_ORG` - organization name (default: my-org)
- `INFLUXDB_BUCKET` - bucket name (default: my-bucket)
- `INFLUXDB_ADMIN_USER` - admin username (default: admin)
- `INFLUXDB_ADMIN_PASSWORD` - admin password (default: password)

Notes and next steps
- The init script uses `influx setup --force` to ensure idempotent setup. Adjust retention and other settings as needed.
- For production, change passwords/tokens and enable secure storage of secrets.
