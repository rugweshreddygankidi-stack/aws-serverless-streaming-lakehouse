# Estimated results

> **Estimated values.** Derived from service limits, the pipeline configuration and typical laptop performance; actual figures vary by machine and environment.

| Metric | Estimated | How the estimate was derived |
|---|---|---|
| Daily volume | 1M events/day | ~12 events/s average; 1 Kinesis shard accepts up to 1,000 records/s, so this is ~1% of one shard |
| Producer rate | ~500 events/s | Default `--rate 500`; PutRecords batches of 500 |
| Lambda invocations | ~1,440/day | 60 s batching window at this volume, so batches are time-bound, not size-bound |
| End-to-end latency p50 / p95 | ~45 s / ~90 s | Up to 60 s batching window plus a few seconds to tens of seconds for the Athena Iceberg insert |
| Dead-lettered events | ~1% of sent | Producer default `--bad-ratio 0.01`; every bad payload fails validation in tests |
| Duplicates in `events_dedup` | 0 | Guaranteed by in-batch dedupe plus the `row_number()` view |
| Cost per million events | ~$1 (range $0.70-$1.50) | Approx. per day: Kinesis shard-hours ~$0.36, Lambda (1,440 x ~15 s x 1 GB) ~$0.36, Athena (1,440 queries x 10 MB minimum) ~$0.07, PUT units + S3 requests ~$0.10 |
| Unit tests | 58 test cases | Counted locally including parametrized cases |

## Notes from the run
_Add 3-5 bullets after your run: what you tuned, what broke, what you learned._
