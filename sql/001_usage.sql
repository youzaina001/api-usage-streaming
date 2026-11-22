BEGIN;
SET LOCAL ROLE installer;

DO $$
BEGIN
    EXECUTE format('REVOKE CONNECT, TEMP ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO installer, usage_consumer, reporting_reader',
        current_database()
    );
END;
$$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA ops;
CREATE SCHEMA warehouse;
CREATE SCHEMA reporting;

CREATE TABLE ops.consumer_checkpoint (
    pipeline_id text PRIMARY KEY,
    topic_id uuid NOT NULL,
    partition integer NOT NULL CHECK (partition = 0),
    next_offset bigint NOT NULL CHECK (next_offset >= 0)
);

CREATE TABLE ops.delivery (
    topic_id uuid NOT NULL,
    partition integer NOT NULL CHECK (partition = 0),
    "offset" bigint NOT NULL CHECK ("offset" >= 0),
    outcome text NOT NULL CHECK (outcome IN ('accepted', 'duplicate', 'conflict', 'invalid')),
    reason varchar(64),
    event_id uuid,
    payload_sha256 bytea CHECK (octet_length(payload_sha256) = 32),
    PRIMARY KEY (topic_id, partition, "offset")
);

CREATE TABLE warehouse.usage_event (
    event_id uuid PRIMARY KEY,
    event_time timestamptz NOT NULL,
    project_id text NOT NULL,
    model_id text NOT NULL,
    input_tokens integer NOT NULL CHECK (input_tokens >= 0),
    output_tokens integer NOT NULL CHECK (output_tokens >= 0),
    topic_id uuid NOT NULL,
    partition integer NOT NULL CHECK (partition = 0),
    "offset" bigint NOT NULL,
    FOREIGN KEY (topic_id, partition, "offset") REFERENCES ops.delivery
);

CREATE VIEW reporting.daily_usage WITH (security_invoker = false) AS
SELECT (event_time AT TIME ZONE 'UTC')::date AS usage_day,
       project_id,
       model_id,
       count(*) AS request_count,
       sum(input_tokens) AS input_tokens,
       sum(output_tokens) AS output_tokens
FROM warehouse.usage_event
GROUP BY 1, 2, 3;

CREATE VIEW reporting.delivery_reconciliation WITH (security_invoker = false) AS
SELECT count(*) AS deliveries,
       count(*) FILTER (WHERE outcome = 'accepted') AS accepted,
       count(*) FILTER (WHERE outcome = 'duplicate') AS duplicate,
       count(*) FILTER (WHERE outcome = 'conflict') AS conflict,
       count(*) FILTER (WHERE outcome = 'invalid') AS invalid
FROM ops.delivery;

GRANT USAGE ON SCHEMA ops, warehouse TO usage_consumer;
GRANT SELECT, INSERT ON ops.delivery, warehouse.usage_event TO usage_consumer;
GRANT SELECT, UPDATE (next_offset) ON ops.consumer_checkpoint TO usage_consumer;
GRANT USAGE ON SCHEMA reporting TO reporting_reader;
GRANT SELECT ON reporting.daily_usage, reporting.delivery_reconciliation TO reporting_reader;

COMMIT;
