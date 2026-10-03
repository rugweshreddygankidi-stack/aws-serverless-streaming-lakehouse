"""Event model, generator and validation for the simulated clickstream feed."""
from __future__ import annotations

import json
import math
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

EVENT_TYPES = ("page_view", "search", "add_to_cart", "purchase", "login")
DEVICES = ("ios", "android", "web", "tablet")
COUNTRIES = ("US", "CA", "GB", "DE", "IN", "BR", "AU")
REQUIRED_FIELDS = ("event_id", "event_type", "user_id", "event_ts")
MAX_FUTURE_SKEW = timedelta(minutes=5)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: Any) -> datetime | None:
    """Parse an ISO-8601 string into an aware UTC datetime, or None if invalid."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        ts = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def validate_event(event: Any, now: datetime | None = None) -> list[str]:
    """Return a list of error codes. An empty list means the event is valid."""
    if not isinstance(event, dict):
        return ["not_an_object"]

    now = now or utcnow()
    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        value = event.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            errors.append(f"missing_{field}")
        elif not isinstance(value, str):
            errors.append(f"invalid_type_{field}")

    event_type = event.get("event_type")
    if isinstance(event_type, str) and event_type and event_type not in EVENT_TYPES:
        errors.append("unknown_event_type")

    ts_raw = event.get("event_ts")
    if isinstance(ts_raw, str) and ts_raw.strip():
        parsed = parse_ts(ts_raw)
        if parsed is None:
            errors.append("invalid_event_ts")
        elif parsed > now + MAX_FUTURE_SKEW:
            errors.append("event_ts_in_future")

    amount = event.get("amount")
    if event_type == "purchase" and amount is None:
        errors.append("missing_amount")
    if amount is not None:
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            errors.append("invalid_type_amount")
        elif not math.isfinite(amount):
            errors.append("invalid_amount")
        elif amount < 0:
            errors.append("negative_amount")

    return errors


def generate_event(rng: random.Random | None = None, now: datetime | None = None) -> dict:
    """Generate one realistic, valid event."""
    rng = rng or random.Random()
    now = now or utcnow()
    event_type = rng.choices(EVENT_TYPES, weights=(60, 15, 12, 5, 8))[0]
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "event_type": event_type,
        "user_id": f"user_{rng.randint(1, 50_000)}",
        "session_id": f"sess_{rng.randint(1, 200_000)}",
        "event_ts": (now - timedelta(milliseconds=rng.randint(0, 2000))).isoformat(),
        "amount": round(rng.uniform(5, 300), 2) if event_type == "purchase" else None,
        "device": rng.choice(DEVICES),
        "country": rng.choice(COUNTRIES),
    }


def make_bad_event(rng: random.Random | None = None) -> str:
    """Return a serialized payload that must be rejected (exercises the dead-letter path)."""
    rng = rng or random.Random()
    event = generate_event(rng)
    kind = rng.choice(["missing_user", "bad_type", "negative", "bad_ts", "malformed"])
    if kind == "missing_user":
        event.pop("user_id")
    elif kind == "bad_type":
        event["event_type"] = "teleport"
    elif kind == "negative":
        event.update(event_type="purchase", amount=-10.0)
    elif kind == "bad_ts":
        event["event_ts"] = "yesterday-ish"
    else:
        return '{"event_id": "oops", '
    return json.dumps(event)
