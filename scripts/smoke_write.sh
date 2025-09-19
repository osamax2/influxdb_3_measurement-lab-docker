#!/usr/bin/env sh
# Simple smoke test: read token from ./run/influx_token and POST LP to /api/v3/write_lp
set -eu
TOKEN_FILE="./run/influx_token"
if [ ! -f "$TOKEN_FILE" ]; then
  echo "ERROR: token file not found: $TOKEN_FILE" >&2
  exit 2
fi
TOKEN=$(sed -n 's/.*"token":"\([^\"]*\)".*/\1/p' "$TOKEN_FILE")
if [ -z "$TOKEN" ]; then
  echo "ERROR: could not extract token from $TOKEN_FILE" >&2
  exit 3
fi
URL="http://127.0.0.1:8086/api/v3/write_lp?db=my-bucket"
LP="smoke_metric,host=smoke value=1"

echo "Using token: ${TOKEN%????}"  # print token prefix only for safety

# Try host-local POST first
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL" \
  -H "Authorization: Token $TOKEN" \
  -H "Content-Type: text/plain; charset=utf-8" \
  --data-binary "$LP" || true)

if [ "$HTTP_CODE" = "204" ]; then
  echo "Write succeeded (host): HTTP $HTTP_CODE"
  exit 0
fi

echo "Host write failed, expected 204 but got: $HTTP_CODE" >&2
echo "Trying write from inside the 'influxdb' container (docker compose exec)" >&2

# Fallback: execute curl inside the container to avoid host networking/proxy differences
docker compose exec influxdb sh -c "curl -s -o /dev/null -w '%{http_code}' -X POST '$URL' -H 'Authorization: Token $TOKEN' -H 'Content-Type: text/plain; charset=utf-8' --data-binary '$LP'" > /tmp/smoke_code || true
CONTAINER_CODE=$(cat /tmp/smoke_code 2>/dev/null || echo "")
rm -f /tmp/smoke_code

if [ "$CONTAINER_CODE" = "204" ]; then
  echo "Write succeeded (container): HTTP $CONTAINER_CODE"
  exit 0
fi

echo "Container write failed, expected 204 but got: ${CONTAINER_CODE:-<no-output>}" >&2
echo "Verbose host diagnostic:" >&2
curl -v -X POST "$URL" -H "Authorization: Token $TOKEN" -H "Content-Type: text/plain; charset=utf-8" --data-binary "$LP" || true

echo "Verbose container diagnostic:" >&2
docker compose exec influxdb sh -c "curl -v -X POST '$URL' -H 'Authorization: Token $TOKEN' -H 'Content-Type: text/plain; charset=utf-8' --data-binary '$LP'" || true

exit 4
