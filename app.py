"""Event ingestion + last-click order attribution + ROAS, on ScyllaDB/Cassandra."""
import os
import zlib
from contextlib import asynccontextmanager
from datetime import date, timedelta, timezone
from pathlib import Path
from typing import Literal

from cassandra.cluster import Cluster
from fastapi import FastAPI, HTTPException
from pydantic import AwareDatetime, BaseModel, Field, model_validator

WINDOW = timedelta(hours=int(os.getenv("ATTRIBUTION_WINDOW_HOURS", "168")))
assert WINDOW <= timedelta(days=30), "clicks_by_user keeps clicks for 30 days (its TTL)"
RAW_BUCKETS = 8  # write-spreading buckets per day in events_by_day
MAX_REPORT_DAYS = 92

Id = Field(min_length=1, max_length=128)


class Event(BaseModel):
    event_id: str = Id  # the idempotency key: producers must reuse it on retry
    type: Literal["impression", "click", "order"]
    event_time: AwareDatetime
    user_id: str = Id
    campaign_id: str | None = Field(None, min_length=1, max_length=128)
    cost_micros: int = Field(0, ge=0)  # what the impression/click cost the advertiser
    revenue_micros: int = Field(0, ge=0)  # order value

    @model_validator(mode="after")
    def campaign_needed_for_ads(self):
        if self.type != "order" and not self.campaign_id:
            raise ValueError("impression and click events need campaign_id")
        return self


QUERIES = {
    "claim": "INSERT INTO event_dedup (event_id, payload, done) VALUES (?, ?, false) IF NOT EXISTS",
    # Plain write after an LWT on the same row: safe here because nothing else ever writes `done`.
    "done": "UPDATE event_dedup SET done = true WHERE event_id = ?",
    "raw": "INSERT INTO events_by_day (day, bucket, event_time, event_id, event_type, payload) VALUES (?, ?, ?, ?, ?, ?)",
    "ad_event": "INSERT INTO ad_events_by_campaign_day (campaign_id, day, event_time, event_id, event_type, user_id, cost_micros) VALUES (?, ?, ?, ?, ?, ?, ?)",
    "click": "INSERT INTO clicks_by_user (user_id, click_time, click_id, campaign_id) VALUES (?, ?, ?, ?)",
    "last_click": "SELECT click_id, click_time, campaign_id FROM clicks_by_user WHERE user_id = ? AND click_time >= ? AND click_time <= ? LIMIT 1",
    "attribute": "INSERT INTO attributed_orders_by_campaign_day (campaign_id, day, order_time, order_id, revenue_micros, click_id, click_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
    "active": "INSERT INTO campaigns_by_day (day, campaign_id) VALUES (?, ?)",
    "campaigns": "SELECT campaign_id FROM campaigns_by_day WHERE day = ?",
    "spend": "SELECT SUM(cost_micros) AS v FROM ad_events_by_campaign_day WHERE campaign_id = ? AND day = ?",
    "revenue": "SELECT SUM(revenue_micros) AS v, COUNT(*) AS n FROM attributed_orders_by_campaign_day WHERE campaign_id = ? AND day = ?",
}
db = None
q = {}


def connect():
    global db
    cluster = Cluster(os.getenv("SCYLLA_HOSTS", "127.0.0.1").split(","))
    db = cluster.connect()
    ddl = Path(__file__).with_name("schema.cql").read_text()
    ddl = "\n".join(l for l in ddl.splitlines() if not l.strip().startswith("--"))
    for stmt in ddl.split(";"):
        if stmt.strip():
            db.execute(stmt)
    db.set_keyspace("adtech")
    q.update({k: db.prepare(v) for k, v in QUERIES.items()})
    return cluster


@asynccontextmanager
async def lifespan(_):
    cluster = connect()
    yield
    cluster.shutdown()


app = FastAPI(title="Pay-per-order attribution", lifespan=lifespan)


@app.post("/events")
def ingest(event: Event):
    claim = db.execute(q["claim"], (event.event_id, event.model_dump_json()))
    if not claim.was_applied:
        row = claim.one()
        if row.done:
            return {"status": "duplicate", "event_id": event.event_id}
        # Claimed earlier but the writes never finished (crash/timeout): finish them with the
        # payload stored first, so a retry with different fields cannot change history.
        event = Event.model_validate_json(row.payload)
    campaign = store(event)
    db.execute(q["done"], (event.event_id,))
    return {"status": "accepted", "event_id": event.event_id, "attributed_campaign": campaign}


def store(e: Event):
    """Every write is an upsert on a key derived from the event, so re-running it is harmless."""
    t = e.event_time.astimezone(timezone.utc)
    day = t.date()
    bucket = zlib.crc32(e.event_id.encode()) % RAW_BUCKETS
    db.execute(q["raw"], (day, bucket, t, e.event_id, e.type, e.model_dump_json()))

    if e.type == "order":
        click = db.execute(q["last_click"], (e.user_id, t - WINDOW, t)).one()
        if click is None:
            return None  # organic order: kept in events_by_day, credited to no campaign
        db.execute(q["attribute"], (click.campaign_id, day, t, e.event_id, e.revenue_micros, click.click_id, click.click_time))
        db.execute(q["active"], (day, click.campaign_id))
        return click.campaign_id

    db.execute(q["ad_event"], (e.campaign_id, day, t, e.event_id, e.type, e.user_id, e.cost_micros))
    db.execute(q["active"], (day, e.campaign_id))
    if e.type == "click":
        db.execute(q["click"], (e.user_id, t, e.event_id, e.campaign_id))
    return None


@app.get("/reports/roas")
def roas(start: date, end: date):
    days = (end - start).days + 1
    if not 1 <= days <= MAX_REPORT_DAYS:
        raise HTTPException(400, f"end must be on/after start, at most {MAX_REPORT_DAYS} days")
    totals = {}
    # shortcut: 2 sequential single-partition queries per campaign-day. Fine for a demo;
    # upgrade to execute_async fan-out or a pre-aggregated daily rollup table at scale.
    for i in range(days):
        day = start + timedelta(days=i)
        for (cid,) in db.execute(q["campaigns"], (day,)):
            t = totals.setdefault(cid, {"spend_micros": 0, "revenue_micros": 0, "orders": 0})
            t["spend_micros"] += db.execute(q["spend"], (cid, day)).one().v or 0
            rev = db.execute(q["revenue"], (cid, day)).one()
            t["revenue_micros"] += rev.v or 0
            t["orders"] += rev.n
    return {
        "start": start, "end": end, "attribution_window_hours": WINDOW // timedelta(hours=1),
        "campaigns": [
            {"campaign_id": cid, **t, "roas": round(t["revenue_micros"] / t["spend_micros"], 4) if t["spend_micros"] else None}
            for cid, t in sorted(totals.items())
        ],
    }
