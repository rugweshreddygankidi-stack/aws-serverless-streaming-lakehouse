import json
import random

import pytest

from lakehouse.events import validate_event
from lakehouse.producer import KINESIS_MAX_BATCH, build_entries, chunked, serialize_event


@pytest.mark.parametrize("size,total,expected_batches", [(500, 1000, 2), (500, 1001, 3), (3, 0, 0), (10, 5, 1)])
def test_chunked_batch_counts(size, total, expected_batches):
    assert len(list(chunked(range(total), size))) == expected_batches


def test_chunked_preserves_order_and_items():
    flat = [x for batch in chunked(range(23), 5) for x in batch]
    assert flat == list(range(23))


def test_serialize_event_is_compact_json():
    payload = serialize_event({"a": 1, "b": None})
    assert payload == b'{"a":1,"b":null}'


def test_build_entries_zero_bad_ratio_all_valid():
    entries = list(build_entries(200, 0.0, random.Random(1)))
    assert len(entries) == 200
    for entry in entries:
        assert validate_event(json.loads(entry["Data"])) == []
        assert entry["PartitionKey"].startswith("user_")


def test_build_entries_all_bad_ratio_uses_bad_key():
    entries = list(build_entries(50, 1.0, random.Random(1)))
    assert all(e["PartitionKey"] == "bad" for e in entries)


def test_build_entries_mix_is_roughly_the_requested_ratio():
    entries = list(build_entries(5000, 0.1, random.Random(5)))
    bad = sum(1 for e in entries if e["PartitionKey"] == "bad")
    assert 350 < bad < 650


def test_kinesis_batch_limit_constant():
    assert KINESIS_MAX_BATCH == 500
