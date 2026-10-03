import base64
import json
from datetime import datetime, timezone

import pytest

from helpers import kinesis_record
from lakehouse import handler
from lakehouse.handler import decode_record, normalise, process_records

NOW = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)


def ev(event_id="e-1", **overrides):
    event = {"event_id": event_id, "event_type": "page_view", "user_id": "u-1",
             "session_id": "s-1", "event_ts": "2026-01-15T11:59:00+00:00",
             "amount": None, "device": "web", "country": "US"}
    event.update(overrides)
    return event


def test_decode_record_roundtrip():
    parsed, raw, err = decode_record(kinesis_record(ev()))
    assert err is None and parsed["event_id"] == "e-1" and "e-1" in raw


def test_decode_record_malformed_json():
    parsed, raw, err = decode_record(kinesis_record('{"event_id": '))
    assert parsed is None and err == "malformed_json" and raw.startswith("{")


def test_decode_record_missing_data():
    assert decode_record({})[2] == "undecodable_payload"


def test_decode_record_invalid_utf8_does_not_crash():
    rec = {"kinesis": {"data": base64.b64encode(b"\xff\xfe\x00").decode()}}
    assert decode_record(rec)[2] == "malformed_json"


def test_process_records_all_valid():
    result = process_records([kinesis_record(ev(f"e-{i}"), str(i)) for i in range(5)], NOW)
    assert len(result.valid) == 5 and not result.invalid and result.duplicates == 0


def test_process_records_routes_invalid_with_sequence_number():
    bad = ev("e-2", event_type="teleport")
    result = process_records([kinesis_record(ev("e-1")), kinesis_record(bad, seq="99")], NOW)
    assert len(result.valid) == 1
    assert result.invalid[0]["sequence_number"] == "99"
    assert "unknown_event_type" in result.invalid[0]["errors"]


def test_process_records_drops_duplicates_within_batch():
    records = [kinesis_record(ev("same")), kinesis_record(ev("same")), kinesis_record(ev("other"))]
    result = process_records(records, NOW)
    assert [r["event_id"] for r in result.valid] == ["same", "other"]
    assert result.duplicates == 1


def test_process_records_malformed_goes_to_dead_letter():
    result = process_records([kinesis_record("not json at all")], NOW)
    assert result.invalid[0]["errors"] == ["malformed_json"]
    assert result.invalid[0]["raw"] == "not json at all"


def test_process_records_empty_batch():
    result = process_records([], NOW)
    assert result.total == 0


def test_total_counts_every_record():
    records = [kinesis_record(ev("a")), kinesis_record(ev("a")), kinesis_record("bad")]
    assert process_records(records, NOW).total == 3


def test_normalise_adds_partition_and_ingest_columns():
    row = normalise(ev(event_ts="2026-01-15T23:30:00-05:00"), NOW)
    assert row["event_date"] == "2026-01-16"  # converted to UTC before taking the date
    assert row["ingested_at"] == NOW.isoformat()


def test_normalise_drops_unknown_fields():
    row = normalise(ev(secret="x"), NOW)
    assert "secret" not in row


def test_to_dataframe_types():
    pd = pytest.importorskip("pandas")
    rows = process_records([kinesis_record(ev("a", event_type="purchase", amount=9.5)),
                            kinesis_record(ev("b"))], NOW).valid
    df = handler.to_dataframe(rows)
    assert str(df["event_ts"].dtype).startswith("datetime64")
    assert df["event_ts"].dt.tz is None
    assert df["amount"].dtype.kind == "f"
    assert pd.isna(df.loc[1, "amount"])


def test_lambda_handler_writes_valid_and_dead_letters(monkeypatch):
    written, dead = [], []
    monkeypatch.setattr(handler, "write_iceberg", lambda rows: written.extend(rows))
    monkeypatch.setattr(handler, "write_dead_letter", lambda rows: dead.extend(rows))
    event = {"Records": [kinesis_record(ev("ok")), kinesis_record("garbage")]}
    summary = handler.lambda_handler(event)
    assert summary == {"received": 2, "written": 1, "dead_lettered": 1, "duplicates_dropped": 0}
    assert len(written) == 1 and len(dead) == 1


def test_lambda_handler_skips_writes_when_nothing_to_write(monkeypatch):
    def boom(rows):
        raise AssertionError("should not be called")

    monkeypatch.setattr(handler, "write_iceberg", boom)
    monkeypatch.setattr(handler, "write_dead_letter", boom)
    assert handler.lambda_handler({"Records": []})["received"] == 0


def test_lambda_handler_propagates_write_errors_for_retry(monkeypatch):
    def fail(rows):
        raise RuntimeError("athena unavailable")

    monkeypatch.setattr(handler, "write_iceberg", fail)
    with pytest.raises(RuntimeError):
        handler.lambda_handler({"Records": [kinesis_record(ev())]})


def test_summary_is_json_serialisable(monkeypatch):
    monkeypatch.setattr(handler, "write_iceberg", lambda rows: None)
    json.dumps(handler.lambda_handler({"Records": [kinesis_record(ev())]}))
