-- One table per query (query-first design). See docs/data-model.md for the reasoning.
-- Statements are split on the semicolon by app.py, so comments must not contain one.

-- SimpleStrategy RF=1 is for the single local node only. In Scylla 6, NetworkTopologyStrategy
-- turns on tablets, which do not support the LWT used by event_dedup. Production: NTS, RF=3, vnodes.
CREATE KEYSPACE IF NOT EXISTS adtech
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Idempotency: one row per event_id, claimed with a lightweight transaction (IF NOT EXISTS).
-- payload keeps the first version seen, done marks that every downstream write finished.
CREATE TABLE IF NOT EXISTS adtech.event_dedup (
  event_id text PRIMARY KEY,
  payload  text,
  done     boolean
) WITH default_time_to_live = 604800;

-- Raw, append-only log of every event (source of truth for replays and backfills).
-- Partition = (UTC day, bucket): bounded by time, and spread over N buckets so one day
-- is never a single hot partition on one replica set.
CREATE TABLE IF NOT EXISTS adtech.events_by_day (
  day        date,
  bucket     int,
  event_time timestamp,
  event_id   text,
  event_type text,
  payload    text,
  PRIMARY KEY ((day, bucket), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};

-- Spend query: all impressions and clicks for one campaign on one day.
CREATE TABLE IF NOT EXISTS adtech.ad_events_by_campaign_day (
  campaign_id text,
  day         date,
  event_time  timestamp,
  event_id    text,
  event_type  text,
  user_id     text,
  cost_micros bigint,
  PRIMARY KEY ((campaign_id, day), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};

-- Attribution lookup: the latest click by this user before the order, inside the window.
-- Newest first, so the lookup is a single-partition slice with LIMIT 1.
-- TTL bounds the partition: clicks older than the max window are useless and expire.
CREATE TABLE IF NOT EXISTS adtech.clicks_by_user (
  user_id     text,
  click_time  timestamp,
  click_id    text,
  campaign_id text,
  PRIMARY KEY ((user_id), click_time, click_id)
) WITH CLUSTERING ORDER BY (click_time DESC, click_id ASC)
  AND default_time_to_live = 2592000;

-- Revenue query: orders credited to one campaign on one day.
CREATE TABLE IF NOT EXISTS adtech.attributed_orders_by_campaign_day (
  campaign_id    text,
  day            date,
  order_time     timestamp,
  order_id       text,
  revenue_micros bigint,
  click_id       text,
  click_time     timestamp,
  PRIMARY KEY ((campaign_id, day), order_time, order_id)
) WITH CLUSTERING ORDER BY (order_time DESC, order_id ASC);

-- Report fan-out: which campaigns had activity on a day (tiny partitions, one row per campaign).
CREATE TABLE IF NOT EXISTS adtech.campaigns_by_day (
  day         date,
  campaign_id text,
  PRIMARY KEY ((day), campaign_id)
);
