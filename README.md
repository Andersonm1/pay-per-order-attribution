# Pay-per-order attribution

Takes in ad **impression**, **click** and **order** events and stores them in **ScyllaDB** (works the
same on Cassandra), running locally in Docker. It drops duplicate events using idempotency keys,
credits each order to the user's last ad click inside a lookback window, and reports **ROAS per
campaign**.

- **[docs/data-model.md](docs/data-model.md)**: partition-key design for time-series events, and how
  idempotency and attribution work. Start here for the NoSQL discussion.
- **[docs/build-guide.md](docs/build-guide.md)**: how the project was built step by step, the decisions
  made, and next steps.

## Quickstart

You need Docker. The first start takes about a minute while Scylla boots.

```bash
docker compose up -d --build --wait
```

Send a click, then an order from the same user:

```bash
curl -X POST localhost:8000/events -H 'content-type: application/json' -d '{"event_id":"clk-1","type":"click","event_time":"2026-03-01T09:00:00Z","user_id":"u1","campaign_id":"spring-sale","cost_micros":500000}'
```

```bash
curl -X POST localhost:8000/events -H 'content-type: application/json' -d '{"event_id":"ord-1","type":"order","event_time":"2026-03-01T12:00:00Z","user_id":"u1","revenue_micros":25000000}'
```

The second call returns `{"status":"accepted","attributed_campaign":"spring-sale"}`. Send it again
and you get `"status":"duplicate"`, and the revenue isn't counted twice.

Get the ROAS report:

```bash
curl "localhost:8000/reports/roas?start=2026-03-01&end=2026-03-07"
```

```json
{"campaigns":[{"campaign_id":"spring-sale","spend_micros":500000,"revenue_micros":25000000,"orders":1,"roas":50.0}]}
```

Run the integration tests (against the real Scylla container):

```bash
docker compose run --rm api python -m pytest -q
```

Interactive API docs are at http://localhost:8000/docs.

## API

### `POST /events`

| Field | Type | Notes |
|---|---|---|
| `event_id` | string | **Idempotency key.** The producer must reuse it when retrying. |
| `type` | `impression` \| `click` \| `order` | |
| `event_time` | ISO-8601 timestamp **with timezone** | Stored as UTC |
| `user_id` | string | Links clicks to orders |
| `campaign_id` | string | Required for impressions and clicks |
| `cost_micros` | int ≥ 0 | Ad spend for the event, in millionths of the currency unit (1 USD = 1,000,000) |
| `revenue_micros` | int ≥ 0 | Order value, same unit |

The response's `status` field:

- **`accepted`**: the event was new and has been stored.
- **`duplicate`**: this `event_id` was already processed, so nothing changed.

For orders, `attributed_campaign` is the credited campaign, or `null` for an organic order.

### `GET /reports/roas?start=YYYY-MM-DD&end=YYYY-MM-DD`

Returns spend, attributed revenue, order count and `roas = revenue / spend` for each campaign over the
date range (UTC days, at most 92). `roas` is `null` when spend is 0.

## Attribution rules

- **Last click wins.** An order is credited to the user's most recent click with
  `order_time - window <= click_time <= order_time`.
- **Window.** `ATTRIBUTION_WINDOW_HOURS` sets it. The default is 168 (7 days) and the maximum is 720
  (30 days, the click retention TTL).
- **No click in the window.** The order is organic: it's stored, but credited to no campaign.

## Layout

```
app.py                  ingest, dedup, attribution, ROAS report
schema.cql              keyspace and tables (applied on startup, safe to re-run)
docker-compose.yml      ScyllaDB 6.2 and the API
tests/                  integration tests against the real database
docs/                   data model and build guide
```
