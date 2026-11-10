# How this project was built, step by step

This guide shows how to build the system from an empty folder, in the order I did it, with the reason
for each decision. Every step leaves something that runs.

## Step 0: Scope it down to the core job

The brief: take in impression, click and order events. Store them in Cassandra/ScyllaDB running in
Docker. Credit orders to ad clicks inside a time window. Drop duplicate events. Report ROAS per campaign.

What's deliberately left out: auth, batching endpoints, a message queue, multi-touch attribution,
dashboards. Each one is a sentence in the "next steps" section below, not code.

**Stack**

- **ScyllaDB 6.2.** It speaks the Cassandra protocol, so everything here runs on Cassandra 4/5 too.
  A single node in Docker starts in about 30 seconds.
- **Python 3.12, FastAPI.** Pydantic validates input where it enters the system (the trust boundary),
  and the HTTP layer takes about 10 lines.
- **`scylla-driver`.** It's ScyllaDB's shard-aware fork of the DataStax driver, with the same
  `cassandra` import path.

## Step 1: Run the database locally

`docker-compose.yml` runs `scylladb/scylla:6.2` with dev-sized flags
(`--smp 1 --memory 750M --overprovisioned 1 --developer-mode 1`) so it fits on a laptop. A
`cqlsh` health check lets the API container wait with `depends_on: condition: service_healthy`
instead of crash-looping until the database is up.

```bash
docker compose up -d scylla
docker compose exec scylla cqlsh -e "DESCRIBE KEYSPACES"
```

## Step 2: Design the tables from the queries

Before writing any code, list every question the system must answer. Then make one table per question,
each answered by a **single partition**. The full reasoning is in [data-model.md](data-model.md). In short:

1. Ad events are partitioned by `(campaign_id, day)`. The partition key buckets time, so no partition
   grows forever.
2. Clicks are partitioned by `user_id`, newest first, with a TTL. The attribution lookup is one seek
   with `LIMIT 1`.
3. Attributed orders are partitioned by `(campaign_id, day)`, the same shape as the spend table, so
   ROAS lines up day by day.
4. A small `campaigns_by_day` index tells the report which partitions to read.
5. The raw log uses `(day, bucket)` so a single day isn't one hot partition.
6. `event_dedup` holds the idempotency key, claimed with `IF NOT EXISTS`.

Write it as `schema.cql` using only `IF NOT EXISTS` statements, so the app can apply it on every start
and running it twice is harmless.

One gotcha found along the way: in Scylla 6, a keyspace using `NetworkTopologyStrategy` gets
**tablets** by default, and tablets don't support LWT. The local keyspace uses `SimpleStrategy`, which
uses vnodes.

## Step 3: The ingest endpoint

`POST /events` takes one event:

```json
{"event_id": "clk-123", "type": "click", "event_time": "2026-03-01T09:01:00Z",
 "user_id": "u1", "campaign_id": "spring-sale", "cost_micros": 500000}
```

Validation rejects these with `422`:

- unknown types
- timestamps without a timezone (`AwareDatetime`)
- impressions or clicks without a campaign
- negative money
- ids longer than 128 characters

Times are stored in UTC, and the day bucket is the UTC date, so a partition boundary never depends on
the server's timezone.

The write path (`app.py`, `ingest` and `store`):

1. **Claim** the `event_id` with an LWT.
   - Already claimed and `done` → return `duplicate` without touching anything else.
   - Claimed but not `done` (a crash or timeout earlier) → finish the job using the **stored first
     payload**.
2. **Write** the raw log row and the query tables. Every write is an upsert on a key derived from the
   event, so writing it again is harmless.
3. **Attribute** if the event is an order: find the latest click in
   `[order_time - window, order_time]`, then write the credited order under that click's campaign.
4. **Mark** the claim `done`.

Statements are prepared once at startup. Prepared statements are parsed once and routed token-aware
straight to a replica.

## Step 4: The ROAS report

`GET /reports/roas?start=2026-03-01&end=2026-03-07` works like this:

- For each day, read `campaigns_by_day`.
- For each (campaign, day), run `SUM(cost_micros)` on the spend partition, and `SUM(revenue_micros)`
  plus `COUNT(*)` on the attributed-orders partition.
- Add up the results per campaign and return `roas = revenue / spend`.

Each aggregate stays inside one partition, so it's cheap. The range is capped at 92 days so one request
can't fan out without limit.

## Step 5: Prove it with integration tests

`tests/test_attribution.py` runs against the real database, because the logic that matters is in the
queries, and a mock would only test the mock. Each run uses fresh ids, so reruns don't collide. The
tests cover:

| Scenario | Expected |
|---|---|
| Two clicks from one user, then an order | Credited to the **latest** click |
| Same click resent with a different cost | `duplicate`, spend unchanged |
| Same order resent | `duplicate`, revenue not doubled |
| Order 8 days after the click (7-day window) | Organic |
| Click *after* the order | Doesn't get credit |
| Claimed but crashed before `done`, then retried with different data | Finishes with the first payload |
| Bad input (no campaign, no timezone, reversed date range) | `422` / `400` |
| ROAS per campaign | Spend $2.00, ROAS 0 (A); spend $0.50, revenue $10, ROAS 20 (B) |

```bash
docker compose run --rm api python -m pytest -q
```

## Step 6: Document and publish

- `README.md`: quickstart, API, project layout.
- `docs/data-model.md`: the partition-key design (the part to talk through in an interview).
- This guide.

Then push to GitHub:

```bash
git init -b main
git add .
git commit -m "Event ingestion and pay-per-order attribution on ScyllaDB"
gh repo create pay-per-order-attribution --private --source . --push
```

## Next steps, in the order I'd do them

1. **Late-click re-attribution job.** Replay `events_by_day` for the last N days and re-run
   attribution. This fixes orders that arrived before their click.
2. **Batch ingest** (`POST /events:batch`) with per-event results. Writes go out with `execute_async`
   but are acknowledged per event.
3. **Kafka in front of the API.** Gives back-pressure and replay. The consumer keys by `event_id`, and
   the database stays the last line of dedup.
4. **Daily rollup table** written by the stream processor, so reports read one row per campaign-day
   instead of summing raw rows.
5. **Other attribution models** (first click, linear, view-through using impressions). The data is
   already there. Only the lookup and the crediting rule change.
6. **Production topology:** 3 nodes, `NetworkTopologyStrategy` RF=3, `LOCAL_QUORUM`, monitoring with
   Scylla Monitoring Stack, and CI that runs the tests against a Scylla service container.
