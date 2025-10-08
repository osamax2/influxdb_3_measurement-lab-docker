import asyncio
import aiohttp
import gzip
import time
import random
import os
import socket
from typing import Optional

# Configuration (override via environment variables)
# Prefer the Compose network service name so tests run reliably inside the
# development compose network by default. Operators can still override with
# INFLUX_URL to point at localhost or another host when desired.
_env_url = os.environ.get("INFLUX_URL")
if _env_url:
    INFLUX_URL = _env_url
else:
    try:
        socket.gethostbyname("influxdb")
        INFLUX_URL = "http://influxdb:8086/api/v2/write"
    except Exception:
        INFLUX_URL = "http://141.45.165.136:8086/api/v2/write"

print(f"Using INFLUX_URL={INFLUX_URL}")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN", "apiv3_testtoken123")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "my-bucket")

# Defaults are small for quick tests; override with env vars for large runs
TOTAL_POINTS = int(os.environ.get("TEST_TOTAL_POINTS", str(1000000)))
BATCH_SIZE = int(os.environ.get("TEST_BATCH_SIZE", str(10000)))
CONCURRENT_BATCHES = int(os.environ.get("TEST_CONCURRENT_BATCHES", str(4)))

REQUEST_TIMEOUT_SEC = int(os.environ.get("TEST_REQUEST_TIMEOUT_SEC", str(60)))
USE_GZIP = os.environ.get("TEST_USE_GZIP", "false").lower() in ("1","true","yes")

def make_line_protocol(start_index: int, batch_size: int) -> str:
    """Generate a batch of points in line protocol format."""
    lines = []
    base_ts = int(time.time() * 1e9)  # nanoseconds
    for i in range(batch_size):
        sensor = random.randint(1, 1000)
        temp = round(random.uniform(10.0, 40.0), 2)
        hum = round(random.uniform(20.0, 100.0), 2)
        ts = base_ts + start_index + i
        line = f"weather,sensor_id={sensor} temperature={temp},humidity={hum} {ts}"
        lines.append(line)
    return "\n".join(lines)

async def post_batch(session: aiohttp.ClientSession, payload: str, batch_count: int, retries: int = 2) -> int:
    """Post one batch to the configured INFLUX_URL with optional gzip and
    simple retries. Returns the number of points successfully written (0 on
    failure).
    """
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Content-Type": "text/plain; charset=utf-8",
        "User-Agent": "influx-test-script/1"
    }
    params = {
        "org": INFLUX_ORG,
        "bucket": INFLUX_BUCKET,
        "precision": "ns"
    }

    if USE_GZIP:
        data = gzip.compress(payload.encode("utf-8"))
        headers["Content-Encoding"] = "gzip"
    else:
        data = payload.encode("utf-8")

    attempt = 0
    while attempt <= retries:
        attempt += 1
        try:
            # Use a per-request timeout to avoid hanging forever
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC)
            async with session.post(INFLUX_URL, params=params, data=data, headers=headers, timeout=timeout) as resp:
                status = resp.status
                if status in (200, 204):
                    return batch_count
                # Read body for diagnostics but cap the length
                text = (await resp.text())[:2000]
                print(f"Attempt {attempt}: Error writing batch: status={status}, body={text}")
        except Exception as ex:
            print(f"Attempt {attempt}: Exception posting batch: {ex}")

        # exponential-ish backoff
        await asyncio.sleep(min(2, 0.1 * attempt * attempt))

    return 0

async def run_writer():
    start = time.time()
    connector = aiohttp.TCPConnector(limit=CONCURRENT_BATCHES * 2)
    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        inflight = []
        written = 0
        # create tasks with explicit batch sizes so we count correctly
        for offset in range(0, TOTAL_POINTS, BATCH_SIZE):
            this_batch = min(BATCH_SIZE, TOTAL_POINTS - offset)
            lp = make_line_protocol(offset, this_batch)
            inflight.append(asyncio.create_task(post_batch(session, lp, this_batch)))
            if len(inflight) >= CONCURRENT_BATCHES:
                results = await asyncio.gather(*inflight)
                inflight.clear()
                written += sum(results)  # results are ints = number of points written
                print(f"Written ~{written}/{TOTAL_POINTS} points")
        if inflight:
            results = await asyncio.gather(*inflight)
            written += sum(results)
    duration = time.time() - start
    print(f"Finished writing ~{written} points (requested {TOTAL_POINTS}) in {duration:.2f}s → ~{written/duration:.0f} pts/s")

if __name__ == "__main__":
    asyncio.run(run_writer())
