-- Kinesis is at-least-once. The Lambda drops duplicates inside a batch, and this view
-- removes any that slip through across batches (e.g. after a retry).
-- Replace <database> with the glue_database Terraform output.
CREATE OR REPLACE VIEW <database>.events_dedup AS
SELECT event_id, event_type, user_id, session_id, event_ts, amount, device, country,
       event_date, ingested_at
FROM (
  SELECT *,
         row_number() OVER (PARTITION BY event_id ORDER BY ingested_at) AS rn
  FROM <database>.events
)
WHERE rn = 1;
