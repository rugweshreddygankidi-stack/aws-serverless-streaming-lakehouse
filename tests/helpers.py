import base64
import json


def kinesis_record(payload, seq="1"):
    """Build a Kinesis Lambda-event record from a dict or raw string."""
    raw = payload if isinstance(payload, str) else json.dumps(payload)
    return {"kinesis": {"data": base64.b64encode(raw.encode()).decode(), "sequenceNumber": seq}}
