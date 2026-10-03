"""Lambda handler: Kinesis batch -> validate -> dedupe -> Iceberg (via Athena) + dead-letter to S3."""
from __future__ import annotations

import base64
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .events import parse_ts, utcnow, validate_event

logger = logging.getLogger()
logger.setLevel(logging.INFO)

COLUMNS = ("event_id", "event_type", "user_id", "session_id", "event_ts", "amount", "device", "country")


@dataclass
class BatchResult:
    valid: list[dict] = field(default_factory=list)
    invalid: list[dict] = field(default_factory=list)
    duplicates: int = 0

    @property
    def total(self) -> int:
        return len(self.valid) + len(self.invalid) + self.duplicates


def decode_record(record: dict) -> tuple[Any, str, str | None]:
    """Decode one Kinesis record. Returns (parsed_json_or_None, raw_text, error_code_or_None)."""
    try:
        raw = base64.b64decode(record["kinesis"]["data"])
    except (KeyError, ValueError):
        return None, "", "undecodable_payload"
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text), text, None
    except json.JSONDecodeError:
        return None, text, "malformed_json"


def normalise(event: dict, ingested_at: datetime) -> dict:
    ts = parse_ts(event["event_ts"])
    row = {col: event.get(col) for col in COLUMNS}
    row["event_ts"] = ts.isoformat()
    row["event_date"] = ts.date().isoformat()
    row["ingested_at"] = ingested_at.isoformat()
    return row


def process_records(records: list[dict], now: datetime | None = None) -> BatchResult:
    """Pure function: classify records into valid / invalid / duplicate."""
    now = now or utcnow()
    result = BatchResult()
    seen: set[str] = set()

    for record in records:
        sequence = record.get("kinesis", {}).get("sequenceNumber")
        parsed, raw, decode_error = decode_record(record)
        errors = [decode_error] if decode_error else validate_event(parsed, now)
        if errors:
            result.invalid.append({"errors": errors, "sequence_number": sequence, "raw": raw})
            continue
        if parsed["event_id"] in seen:
            result.duplicates += 1
            continue
        seen.add(parsed["event_id"])
        result.valid.append(normalise(parsed, now))
    return result


def to_dataframe(rows: list[dict]):
    import pandas as pd

    df = pd.DataFrame(rows)
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True).dt.tz_localize(None)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True).dt.tz_localize(None)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    return df


def write_iceberg(rows: list[dict]) -> None:
    import awswrangler as wr  # provided by the AWS SDK for pandas Lambda layer

    bucket = os.environ["LAKEHOUSE_BUCKET"]
    table = os.environ["ICEBERG_TABLE"]
    wr.athena.to_iceberg(
        df=to_dataframe(rows),
        database=os.environ["ICEBERG_DATABASE"],
        table=table,
        table_location=f"s3://{bucket}/iceberg/{table}/",
        temp_path=f"s3://{bucket}/athena-temp/",
        workgroup=os.environ["ATHENA_WORKGROUP"],
        partition_cols=["event_date"],
        keep_files=False,
    )


def write_dead_letter(rows: list[dict]) -> None:
    import boto3

    bucket = os.environ["LAKEHOUSE_BUCKET"]
    now = datetime.now(timezone.utc)
    key = f"dead-letter/dt={now:%Y-%m-%d}/{now:%H%M%S}-{uuid.uuid4().hex}.jsonl"
    body = "\n".join(json.dumps(r) for r in rows)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def lambda_handler(event: dict, context: Any = None) -> dict:
    records = event.get("Records", [])
    result = process_records(records)

    # Raise on write failure so the Kinesis event source mapping retries / bisects the batch.
    if result.valid:
        write_iceberg(result.valid)
    if result.invalid:
        write_dead_letter(result.invalid)

    summary = {
        "received": len(records),
        "written": len(result.valid),
        "dead_lettered": len(result.invalid),
        "duplicates_dropped": result.duplicates,
    }
    logger.info(json.dumps({"metric": "batch_summary", **summary}))
    return summary
