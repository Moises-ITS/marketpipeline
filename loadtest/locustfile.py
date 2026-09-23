"""Load test for the read path. This is the evidence behind the numbers in RESULTS.md.

WHY LOCUST AND NOT `curl -w '%{time_total}'`
A single request measures an idle server. It cannot show queueing, connection-pool exhaustion,
or tail latency - which is where real systems actually break. A p50 of 2ms with a p99 of 900ms
is a bad system, and only concurrency reveals that gap.

WHAT IS MEASURED
Two endpoints, deliberately:
  /prices/[symbol]          the hot path - one Redis HGETALL
  /prices/[symbol]/history  the tier that the optimization moves (TimescaleDB -> Redis Streams)

RUN IT (with the API and the ingest worker already running):
    .venv\\Scripts\\locust -f loadtest/locustfile.py --headless -u 100 -r 20 -t 60s ^
        --host http://localhost:8000 --csv loadtest/results/after

The before/after procedure is documented in RESULTS.md - it is the same command run against
the API started with HISTORY_FORCE_DB=true and then false.
"""

import random

from locust import HttpUser, between, tag, task

SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD"]


class ReadUser(HttpUser):
    # A tiny think-time. Zero would measure how fast Locust itself can spin, not how the API
    # behaves under a realistic arrival pattern.
    wait_time = between(0.01, 0.05)

    # Tags let one endpoint be measured alone: `--tags history` isolates the tier
    # comparison from the hot-path traffic it would otherwise be competing with.
    @tag("hot")
    @task(3)
    def latest_price(self):
        # `name=` groups every symbol under one row in the report. Without it the statistics
        # are split three ways and each percentile is computed from a third of the samples.
        symbol = random.choice(SYMBOLS)
        self.client.get(f"/prices/{symbol}", name="/prices/[symbol]")

    @tag("history")
    @task(1)
    def recent_history(self):
        # 30s is inside the Redis Streams window in normal operation, which is precisely what
        # makes it the request the optimization affects.
        symbol = random.choice(SYMBOLS)
        self.client.get(
            f"/prices/{symbol}/history?window=30s&limit=100",
            name="/prices/[symbol]/history",
        )

    @tag("health")
    @task(1)
    def health(self):
        self.client.get("/health")
