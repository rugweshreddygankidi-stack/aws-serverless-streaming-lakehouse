"""Send simulated events to Kinesis.

Example:
    PYTHONPATH=src python -m lakehouse.producer --stream <stream-name> --events 100000 --rate 500
"""
from __future__ import annotations

import argparse
import json
import random
import time
from collections.abc import Iterable, Iterator

from .events import generate_event, make_bad_event

KINESIS_MAX_BATCH = 500  # PutRecords hard limit


def serialize_event(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def chunked(items: Iterable, size: int) -> Iterator[list]:
    batch: list = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def build_entries(count: int, bad_ratio: float, rng: random.Random) -> Iterator[dict]:
    for _ in range(count):
        if rng.random() < bad_ratio:
            yield {"Data": make_bad_event(rng).encode("utf-8"), "PartitionKey": "bad"}
        else:
            event = generate_event(rng)
            yield {"Data": serialize_event(event), "PartitionKey": event["user_id"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--rate", type=int, default=500, help="target events per second")
    parser.add_argument("--bad-ratio", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    import boto3

    client = boto3.client("kinesis")
    rng = random.Random(args.seed)
    sent = failed = 0
    started = time.monotonic()

    for batch in chunked(build_entries(args.events, args.bad_ratio, rng), min(KINESIS_MAX_BATCH, args.rate)):
        response = client.put_records(StreamName=args.stream, Records=batch)
        failed += response.get("FailedRecordCount", 0)
        sent += len(batch) - response.get("FailedRecordCount", 0)
        expected = sent / args.rate  # simple pacing to hold the target rate
        elapsed = time.monotonic() - started
        if expected > elapsed:
            time.sleep(expected - elapsed)

    elapsed = time.monotonic() - started
    print(json.dumps({"sent": sent, "failed": failed, "seconds": round(elapsed, 1),
                      "events_per_sec": round(sent / elapsed, 1) if elapsed else None}))


if __name__ == "__main__":
    main()
