-- Replace <database> with the glue_database Terraform output.

-- 1. Row count (compare with what the producer reported as "sent")
SELECT count(*) AS events FROM <database>.events_dedup;

-- 2. Events per hour
SELECT date_trunc('hour', event_ts) AS hour, count(*) AS events
FROM <database>.events_dedup
GROUP BY 1 ORDER BY 1;

-- 3. Daily revenue and purchase count (partition pruning on event_date)
SELECT event_date, count(*) AS purchases, round(sum(amount), 2) AS revenue
FROM <database>.events_dedup
WHERE event_type = 'purchase' AND event_date >= cast(current_date - interval '7' day AS varchar)
GROUP BY 1 ORDER BY 1;

-- 4. End-to-end latency: producer timestamp -> row written to the lakehouse
SELECT approx_percentile(date_diff('second', event_ts, ingested_at), 0.5)  AS p50_seconds,
       approx_percentile(date_diff('second', event_ts, ingested_at), 0.95) AS p95_seconds
FROM <database>.events_dedup;

-- 5. Iceberg maintenance: compact small files created by frequent small batches
OPTIMIZE <database>.events REWRITE DATA USING BIN_PACK;
VACUUM <database>.events;
