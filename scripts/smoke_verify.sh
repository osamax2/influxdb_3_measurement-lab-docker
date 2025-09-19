#!/usr/bin/env sh
# Verify that points exist in the configured bucket using the v3 SQL query API.
# Tries host first, falls back to executing curl inside the influxdb container.

set -eu

TOKEN_FILE="./run/influx_token"

read_token() {
  if [ -n "${INFLUX_TOKEN-}" ]; then
    # env may contain JSON or raw token
    tok=$(printf '%s' "$INFLUX_TOKEN" | sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"']\+\)".*/\1/p' || true)
    if [ -n "$tok" ]; then printf '%s' "$tok"; return 0; fi
    printf '%s' "$INFLUX_TOKEN"; return 0
  fi

  if [ -f "$TOKEN_FILE" ]; then
    tok=$(sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"']\+\)".*/\1/p' "$TOKEN_FILE" || true)
    if [ -n "$tok" ]; then printf '%s' "$tok"; return 0; fi
    cat "$TOKEN_FILE"; return 0
  fi
  return 1
}

token=$(read_token || true)
if [ -z "$token" ]; then
  echo "No token found in INFLUX_TOKEN env or $TOKEN_FILE" >&2
  exit 2
fi

INFLUX_HOST=${INFLUX_HOST:-localhost}
INFLUX_PORT=${INFLUX_PORT:-8181}
DB=${INFLUX_DB:-${INFLUX_BUCKET:-my-bucket}}

Q="SELECT * FROM \"${DB}\" LIMIT 1"

do_host_query() {
  url="http://$INFLUX_HOST:$INFLUX_PORT/api/v3/query_sql"
  body=$(printf '{"db":"%s","q":"%s","format":"json"}' "$DB" "$Q")
  resp=$(curl -sS -w "\n%{http_code}" -X POST "$url" -H "Authorization: Bearer $token" -H "Content-Type: application/json" --data-binary "$body" 2>/dev/null || true)
  http=$(printf '%s' "$resp" | tail -n1)
  payload=$(printf '%s' "$resp" | sed '$d')
  if [ "$http" = "200" ] && printf '%s' "$payload" | grep -q '#!/usr/bin/env sh
# Verify that points exist in the configured bucket using the v3 SQL query API.
# Tries host first, falls back to executing curl inside the influxdb container.

set -eu

ROOT_RUN_DIR="./run"
TOKEN_FILE="${ROOT_RUN_DIR}/influx_token"

read_token_from_file() {
  if [ -f "$TOKEN_FILE" ]; then
    # file may contain JSON {"token":"apiv3_..."} or raw token
    tok=$(sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"]\+\)".*/\1/p' "$TOKEN_FILE" || true)
    if [ -n "$tok" ]; then
      echo "$tok"
      return 0
    fi
    # fallback to raw content
    cat "$TOKEN_FILE"
    return 0
  fi
  return 1
}

get_token() {
  if [ -n "${INFLUX_TOKEN-}" ]; then
    # env may contain JSON or raw token
    echo "$INFLUX_TOKEN" | sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"]\+\)".*/\1/p' || true
    # if previous sed produced nothing, print env raw
    if [ "$(echo "$INFLUX_TOKEN" | sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"]\+\)".*/\1/p')" = "" ]; then
      #!/usr/bin/env sh
      # Verify that points exist in the configured bucket using the v3 SQL query API.
      # Tries host first, falls back to executing curl inside the influxdb container.

      set -eu

      ROOT_RUN_DIR="./run"
      TOKEN_FILE="${ROOT_RUN_DIR}/influx_token"

      read_token_from_file() {
        if [ -f "$TOKEN_FILE" ]; then
          # file may contain JSON {"token":"apiv3_..."} or raw token
          tok=$(sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"\]\+\)".*/\1/p' "$TOKEN_FILE" || true)
          if [ -n "$tok" ]; then
            echo "$tok"
            return 0
          fi
          # fallback to raw content
          cat "$TOKEN_FILE"
          return 0
        fi
        return 1
      }

      get_token() {
        if [ -n "${INFLUX_TOKEN-}" ]; then
          # env may contain JSON or raw token
          tok=$(echo "$INFLUX_TOKEN" | sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"\]\+\)".*/\1/p' || true)
          if [ -n "$tok" ]; then
            echo "$tok"
          else
            #!/usr/bin/env sh
            # Verify that points exist in the configured bucket using the v3 SQL query API.
            # Tries host first, falls back to executing curl inside the influxdb container.

            set -eu

            ROOT_RUN_DIR="./run"
            TOKEN_FILE="${ROOT_RUN_DIR}/influx_token"

            read_token_from_file() {
              if [ -f "$TOKEN_FILE" ]; then
                # file may contain JSON {"token":"apiv3_..."} or raw token
                tok=$(sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"']\+\)".*/\1/p' "$TOKEN_FILE" || true)
                if [ -n "$tok" ]; then
                  echo "$tok"
                  return 0
                fi
                # fallback to raw content
                cat "$TOKEN_FILE"
                return 0
              fi
              return 1
            }

            get_token() {
              if [ -n "${INFLUX_TOKEN-}" ]; then
                # env may contain JSON or raw token
                tok=$(printf '%s' "$INFLUX_TOKEN" | sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"']\+\)".*/\1/p' || true)
                if [ -n "$tok" ]; then
                  echo "$tok"
                else
                  echo "$INFLUX_TOKEN"
                fi
                return 0
              fi

              if token=$(read_token_from_file 2>/dev/null); then
                echo "$token"
                return 0
              fi

              return 1
            }

            token=$(get_token || true)
            if [ -z "$token" ]; then
              echo "No token found in INFLUX_TOKEN env or $TOKEN_FILE" >&2
              exit 2
            fi

            INFLUX_HOST=${INFLUX_HOST:-localhost}
            INFLUX_PORT=${INFLUX_PORT:-8181}
            DB=${INFLUX_DB:-${INFLUX_BUCKET:-my-bucket}}

            # SQL query to check for any point in the bucket (limit 1)
            Q="SELECT * FROM \"${DB}\" LIMIT 1"

            do_query() {
              base_url="$1"
              # use POST JSON to /api/v3/query_sql
              resp=$(curl -sS -w "\n%{http_code}" -X POST "$base_url/api/v3/query_sql" \
                -H "Authorization: Bearer $token" \
                -H "Content-Type: application/json" \
                --data "{\"db\": \"$DB\", \"q\": \"$Q\", \"format\": \"json\"}"
              ) || return 1

              http_code=$(printf "%s" "$resp" | tail -n1)
              body=$(printf "%s" "$resp" | sed '$d')

              if [ "$http_code" != "200" ]; then
                printf "Query failed (%s): %s\n" "$http_code" "$body" >&2
                return 1
              fi

              # naive check: body non-empty and not just {"results":[]}
              if printf "%s" "$body" | grep -q '"results"'; then
                # check for any rows
                if printf "%s" "$body" | grep -q '"results"\s*:\s*\['; then
                  return 0
                fi
              fi

              return 1
            }

            # Try host endpoint first
            host_url="http://$INFLUX_HOST:$INFLUX_PORT"
            if do_query "$host_url"; then
              echo "Verification succeeded via host $host_url";
              exit 0
            fi

            echo "Host query failed, trying inside influxdb container..." >&2

            if docker compose ps influxdb >/dev/null 2>&1; then
              json_payload=$(printf '{"db":"%s","q":"%s","format":"json"}' "$DB" "$Q")
              # pass token via env into the container to avoid shell interpolation issues
              echo "$json_payload" | docker compose exec -e TOKEN="$token" -T influxdb sh -c 'cat >/tmp/verify_query.json && curl -sS -X POST http://127.0.0.1:8181/api/v3/query_sql -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" --data-binary @/tmp/verify_query.json | jq -e ".results | length > 0"' >/dev/null 2>&1 && echo "Verification succeeded inside container" && exit 0 || (
                echo "Container query failed" >&2; exit 1
              )
            else
              echo "docker compose doesn't report influxdb container; cannot run container fallback" >&2
              exit 1
            fi
