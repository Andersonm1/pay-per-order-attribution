"""Integration tests: need a running ScyllaDB (`docker compose up -d scylla`)."""
import uuid

import pytest
from fastapi.testclient import TestClient

from app import app

RUN = uuid.uuid4().hex[:8]  # fresh ids per run, so reruns against the same DB don't collide


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def ev(client, kind, eid, t, user, campaign=None, cost=0, revenue=0):
    body = {"event_id": f"{RUN}-{eid}", "type": kind, "event_time": t, "user_id": f"{RUN}-{user}",
            "campaign_id": campaign and f"{RUN}-{campaign}", "cost_micros": cost, "revenue_micros": revenue}
    r = client.post("/events", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def test_attribution_dedup_and_roas(client):
    # Campaign A: an impression ($1.00) and a click ($0.50) from user u1.
    ev(client, "impression", "imp1", "2026-03-01T09:00:00Z", "u1", "A", cost=1_000_000)
    ev(client, "click", "clk1", "2026-03-01T09:01:00Z", "u1", "A", cost=500_000)
    # Same user later clicks campaign B ($0.50): last click wins.
    ev(client, "click", "clk2", "2026-03-01T10:00:00Z", "u1", "B", cost=500_000)

    # Retry of clk2 (same idempotency key, even with a different cost) is a no-op.
    assert ev(client, "click", "clk2", "2026-03-01T10:00:00Z", "u1", "B", cost=9_000_000)["status"] == "duplicate"

    # u1 orders $10 within the window -> credited to B, the most recent click.
    assert ev(client, "order", "ord1", "2026-03-01T12:00:00Z", "u1", revenue=10_000_000)["attributed_campaign"] == f"{RUN}-B"
    # Retried order: no double revenue.
    assert ev(client, "order", "ord1", "2026-03-01T12:00:00Z", "u1", revenue=10_000_000)["status"] == "duplicate"

    # u2 clicks A, then orders 8 days later: outside the 7-day window, so unattributed.
    ev(client, "click", "clk3", "2026-03-01T08:00:00Z", "u2", "A", cost=500_000)
    assert ev(client, "order", "ord2", "2026-03-09T09:00:00Z", "u2", revenue=50_000_000)["attributed_campaign"] is None

    # A click *after* the order never gets credit for it.
    ev(client, "click", "clk4", "2026-03-02T12:00:00Z", "u3", "A", cost=0)
    assert ev(client, "order", "ord3", "2026-03-02T11:00:00Z", "u3", revenue=1_000_000)["attributed_campaign"] is None

    r = client.get("/reports/roas", params={"start": "2026-03-01", "end": "2026-03-09"})
    assert r.status_code == 200
    rows = {c["campaign_id"]: c for c in r.json()["campaigns"] if c["campaign_id"].startswith(RUN)}
    assert rows[f"{RUN}-A"] == {"campaign_id": f"{RUN}-A", "spend_micros": 2_000_000, "revenue_micros": 0, "orders": 0, "roas": 0.0}
    assert rows[f"{RUN}-B"] == {"campaign_id": f"{RUN}-B", "spend_micros": 500_000, "revenue_micros": 10_000_000, "orders": 1, "roas": 20.0}


def test_retry_after_crash_finishes_with_first_payload(client):
    import app as svc
    # Simulate a crash right after the idempotency claim: row exists, done = false, no data written.
    first = svc.Event(event_id=f"{RUN}-crash", type="click", event_time="2026-04-01T09:00:00Z",
                      user_id=f"{RUN}-u9", campaign_id=f"{RUN}-C", cost_micros=700_000)
    svc.db.execute(svc.q["claim"], (first.event_id, first.model_dump_json()))
    # The retry carries a different cost, but the stored first payload must win.
    assert ev(client, "click", "crash", "2026-04-01T09:00:00Z", "u9", "C", cost=1)["status"] == "accepted"
    assert ev(client, "click", "crash", "2026-04-01T09:00:00Z", "u9", "C", cost=1)["status"] == "duplicate"
    r = client.get("/reports/roas", params={"start": "2026-04-01", "end": "2026-04-01"}).json()
    assert [c["spend_micros"] for c in r["campaigns"] if c["campaign_id"] == f"{RUN}-C"] == [700_000]


def test_validation(client):
    bad = {"event_id": "x", "type": "click", "event_time": "2026-03-01T09:00:00Z", "user_id": "u"}
    assert client.post("/events", json=bad).status_code == 422  # click without campaign
    naive = {**bad, "campaign_id": "A", "event_time": "2026-03-01T09:00:00"}
    assert client.post("/events", json=naive).status_code == 422  # timestamp without timezone
    assert client.get("/reports/roas", params={"start": "2026-03-02", "end": "2026-03-01"}).status_code == 400
