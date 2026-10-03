import random
from datetime import datetime, timedelta, timezone

import pytest

from lakehouse.events import (EVENT_TYPES, generate_event, make_bad_event, parse_ts, validate_event)

NOW = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)


def good_event(**overrides):
    event = {
        "event_id": "e-1", "event_type": "page_view", "user_id": "u-1",
        "event_ts": NOW.isoformat(), "amount": None,
    }
    event.update(overrides)
    return event


def test_valid_event_has_no_errors():
    assert validate_event(good_event(), NOW) == []


@pytest.mark.parametrize("field", ["event_id", "event_type", "user_id", "event_ts"])
def test_missing_required_field(field):
    event = good_event()
    event.pop(field)
    assert f"missing_{field}" in validate_event(event, NOW)


@pytest.mark.parametrize("field", ["event_id", "user_id"])
def test_blank_required_field(field):
    assert f"missing_{field}" in validate_event(good_event(**{field: "   "}), NOW)


def test_wrong_type_for_required_field():
    assert "invalid_type_user_id" in validate_event(good_event(user_id=123), NOW)


def test_unknown_event_type():
    assert "unknown_event_type" in validate_event(good_event(event_type="teleport"), NOW)


@pytest.mark.parametrize("value", ["yesterday", "2026-13-45T00:00:00", "12:00"])
def test_invalid_timestamp(value):
    assert "invalid_event_ts" in validate_event(good_event(event_ts=value), NOW)


def test_future_timestamp_beyond_skew_rejected():
    future = (NOW + timedelta(minutes=30)).isoformat()
    assert "event_ts_in_future" in validate_event(good_event(event_ts=future), NOW)


def test_small_future_skew_tolerated():
    near = (NOW + timedelta(minutes=2)).isoformat()
    assert validate_event(good_event(event_ts=near), NOW) == []


def test_purchase_requires_amount():
    assert "missing_amount" in validate_event(good_event(event_type="purchase"), NOW)


def test_purchase_with_amount_ok():
    assert validate_event(good_event(event_type="purchase", amount=19.99), NOW) == []


@pytest.mark.parametrize("amount,code", [(-1, "negative_amount"), ("12", "invalid_type_amount"),
                                         (True, "invalid_type_amount"), (float("inf"), "invalid_amount")])
def test_bad_amounts(amount, code):
    assert code in validate_event(good_event(event_type="purchase", amount=amount), NOW)


@pytest.mark.parametrize("value", [None, [], "text", 42])
def test_non_object_rejected(value):
    assert validate_event(value, NOW) == ["not_an_object"]


def test_naive_timestamp_treated_as_utc():
    ts = parse_ts("2026-01-15T12:00:00")
    assert ts == NOW


def test_parse_ts_converts_offsets_to_utc():
    assert parse_ts("2026-01-15T14:00:00+02:00") == NOW


def test_parse_ts_rejects_non_strings():
    assert parse_ts(None) is None
    assert parse_ts(123) is None


def test_generated_events_are_valid():
    rng = random.Random(7)
    for _ in range(500):
        assert validate_event(generate_event(rng)) == []


def test_generator_is_deterministic_with_seed():
    now = NOW
    assert generate_event(random.Random(1), now) == generate_event(random.Random(1), now)


def test_generator_covers_all_event_types():
    rng = random.Random(3)
    seen = {generate_event(rng)["event_type"] for _ in range(2000)}
    assert seen == set(EVENT_TYPES)


def test_bad_events_are_rejected_by_pipeline_rules():
    import json

    rng = random.Random(11)
    for _ in range(200):
        payload = make_bad_event(rng)
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            continue  # malformed JSON is rejected at decode time
        assert validate_event(parsed) != []
