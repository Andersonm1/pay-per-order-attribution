# Data model: partition-key design for time-series events

This is the NoSQL part of the project: why the tables look the way they do, and why that shape suits
event data. The DDL is in [`schema.cql`](../schema.cql).

## 1. The two rules that drive everything

Cassandra and ScyllaDB store data by **partition key**. The key is hashed onto a token ring, and the
hash decides which nodes own the partition. Inside a partition, rows are sorted on disk by the
**clustering columns**. Two rules follow from that:

1. **A fast query reads one partition** (or a few partitions you can name). You can't join, and
   scanning the whole cluster is not a query you run on a request path.
2. **A partition must stay bounded.** A healthy partition is well under ~100 MB and a few hundred
   thousand rows. If a partition keeps growing, it becomes a *hot* partition: one replica set takes all
   the writes, compaction and repair slow down, and reads time out.

So the design works backwards: **list the queries first, then build one table per query**. Data is
duplicated on purpose (denormalized), because disk is cheap and cross-partition reads are expensive.

## 2. Queries this system answers

| # | Query | Table |
|---|-------|-------|
| Q1 | Have I already seen event `X`? (idempotency) | `event_dedup` |
| Q2 | Replay or backfill every event for a day | `events_by_day` |
| Q3 | What did campaign `C` spend on day `D`? | `ad_events_by_campaign_day` |
| Q4 | What was user `U`'s latest click before time `T`, within the window? | `clicks_by_user` |
| Q5 | What revenue was credited to campaign `C` on day `D`? | `attributed_orders_by_campaign_day` |
| Q6 | Which campaigns were active on day `D`? | `campaigns_by_day` |

Every one of these reads **a single partition**. The ROAS report is Q6 for each day, then Q3 and Q5 for
each (campaign, day).

## 3. Why time-series data wants a *compound* partition key

The obvious first idea, `PRIMARY KEY (campaign_id, event_time)`, is wrong for event data:

- **It never stops growing.** A campaign that runs for a year puts every impression into one partition.
  Impressions are the highest-volume event type, so that can be millions of rows a day.
- **All writes for a big campaign land on the same replicas.** The ring's load balance only works when
  writes spread over many partition keys.

The fix is to **bucket time into the partition key**:

```sql
PRIMARY KEY ((campaign_id, day), event_time, event_id)
```

- `(campaign_id, day)` is the partition key. Each campaign gets a new partition every UTC day, so a
  partition holds at most one day of one campaign's events. That size is known in advance.
- `event_time` is the first clustering column, so rows in the partition are stored in time order.
  Reading "the last hour" is a sequential slice on disk, not a scan.
- `event_id` is the last clustering column. It makes the primary key unique: two events in the same
  millisecond don't overwrite each other, and a retried event overwrites *itself*. That second point
  matters for idempotency (section 5).

**Choosing the bucket size.** Pick the bucket so one partition stays bounded at your peak rate:

| Rate per campaign | Bucket |
|---|---|
| Up to ~1M events/day | `day` (this project) |
| Higher | `hour`, or `(campaign_id, day, shard)` where `shard = hash(event_id) % N` |

The cost of smaller buckets is more partitions to read for a range query. A 7-day report on daily
buckets is 7 reads per campaign; on hourly buckets it's 168. That is the main trade-off to talk through.

### Spreading one hot day: `events_by_day`

The raw log, keyed only by time, has the same problem at a bigger scale: *every* event for a day would
go into one partition. So it adds a synthetic **bucket**:

```sql
PRIMARY KEY ((day, bucket), event_time, event_id)  -- bucket = crc32(event_id) % 8
```

That gives 8 partitions per day, spread around the ring. The bucket comes from a deterministic hash of the
event id, so a retried event always lands in the same bucket (Python's built-in `hash()` changes per
process, so it would break that). A replay job reads `(day, 0..7)` in parallel.

## 4. Other time-series choices in the schema

- **`CLUSTERING ORDER BY (event_time DESC)`.** Most reads want the newest data. The attribution lookup
  (Q4) needs *the latest click at or before T*. With descending order that's
  `WHERE user_id = ? AND click_time >= T-window AND click_time <= T LIMIT 1`. It's one seek and returns
  the first row it finds.
- **TTL as retention.** A click can only earn credit inside the attribution window, so
  `clicks_by_user` has `default_time_to_live = 30 days`. That's the maximum window the app accepts, and
  `app.py` asserts it at startup. Old clicks expire on their own, which also bounds each user's
  partition. `event_dedup` uses a 7-day TTL: retries older than a week aren't expected.
- **TimeWindowCompactionStrategy** on the append-only event tables. TWCS groups SSTables by write time
  (one window per day here), so an old day is compacted once and then left alone. Expired data drops a
  whole SSTable at a time instead of being rewritten. That suits write-once, time-ordered data. (It
  pairs best with TTLs. Raw events here keep no TTL so they can be replayed. In production you'd add a
  retention TTL and archive to object storage.)
- **No counters.** Running totals in `counter` columns look tempting for spend and revenue, but counter
  increments are **not idempotent**: a retried `+1` counts twice. Instead, the raw rows are stored with
  deterministic keys and summed per partition with `SUM()` at read time. A retry rewrites the same row,
  so it can never inflate the total.
- **Money is `bigint` micros** (1 USD = 1,000,000). No floating-point rounding in sums.

## 5. Idempotency (deduplication)

Producers send at-least-once, so the same event can show up twice: client retries, load-balancer
replays, queue redelivery. Two layers stop it from being counted twice:

1. **Every write is an upsert on a key derived from the event** (`event_id`, `event_time`, ...). Writing
   the same event twice produces the same row. That's free idempotency, and it's why the schema has no
   counters.
2. **A claim with a lightweight transaction (LWT)** in `event_dedup`:

   ```sql
   INSERT INTO event_dedup (event_id, payload, done) VALUES (?, ?, false) IF NOT EXISTS
   ```

   LWT runs Paxos among the replicas, so exactly one request wins the claim. Layer 1 alone isn't enough
   for two reasons. A retry with a *different* `event_time` would land at a different clustering
   position and create a second row. And attribution has a side effect that should run once.

**Crash safety.** The flow is: claim, then write the data, then set `done = true`. If the process dies
between the claim and `done`, the retry sees `done = false`. It then **finishes the writes using the
payload stored at claim time**, not the payload in the retry. The event is never lost (the trap of
"claim first, write later" designs) and never changed by a retry. Once `done` is set, retries return
`"status": "duplicate"` straight away.

**Cost.** An LWT takes several Paxos round trips instead of 1, so it costs most of the ingest latency. At very
high volume you'd dedupe upstream instead (for example Kafka with idempotent producers and a stream
processor keyed by `event_id`) and rely on layer 1 alone in the database.

## 6. Attribution model

- **Last click, within a lookback window.** The default window is 7 days, set by
  `ATTRIBUTION_WINDOW_HOURS`. When an order arrives, the app finds the user's latest click with
  `order_time - window <= click_time <= order_time` and credits the order's revenue to that click's
  campaign.
- A click *after* the order never gets credit. An order with no click in the window is **organic**: it's
  kept in `events_by_day` and credited to no campaign.
- The credited order is written to `attributed_orders_by_campaign_day` under the **order's** day. Spend
  is counted under each ad event's own day.
- **ROAS = attributed revenue / spend** for the report's date range. It's `null` when spend is 0.

**Known limit: late clicks.** Attribution runs when the order arrives. If a click shows up *after* an
order it should have been credited to (because of clock skew or a delayed pipeline), the order stays
organic. The fix is a re-attribution job that replays `events_by_day` for recent days. The raw log
exists partly for that.

## 7. What changes at production scale

| Here | Production |
|------|------------|
| 1 node, `SimpleStrategy`, RF=1 | 3+ nodes per DC, `NetworkTopologyStrategy`, RF=3, `LOCAL_QUORUM` |
| Vnodes (tablets in Scylla 6.x don't support LWT) | Same, or move dedup out of the database |
| Report sums partitions at read time | Stream processor writes pre-aggregated daily rollups |
| Sequential report queries | `execute_async` fan-out |
| Attribute on order arrival | Plus a re-attribution job for late clicks |
