# API Usage Streaming Pipeline

Reconcile synthetic API usage events from one Kafka partition into daily request and token totals in PostgreSQL. Each batch commits accepted facts, one outcome per delivered record, and the next Kafka offset in a single transaction.

## Run the local demo

From the repository root, with Docker Compose and uv installed:

```bash
uv sync --locked
docker compose --env-file /dev/null -p api-usage-demo up -d --wait
uv run --locked usage-stream setup
uv run --locked usage-stream publish-fixture
uv run --locked usage-stream consume --until-high-watermark --timeout-seconds 30
```

Kafka can take a moment to accept requests after the container starts. If setup reports a broker error during startup, rerun setup once Kafka is ready. Setup preserves a matching checkpoint when run again.

Query both views as the reporting login:

```bash
docker compose --env-file /dev/null -p api-usage-demo exec -T \
  -e PGPASSWORD=usage-reporting-local postgres \
  psql -h 127.0.0.1 -U reporting_reader -d usage -v ON_ERROR_STOP=1 <<'SQL'
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT * FROM reporting.daily_usage ORDER BY usage_day;
SELECT * FROM reporting.delivery_reconciliation;
COMMIT;
SQL
```

| UTC day | Requests | Input tokens | Output tokens |
| --- | ---: | ---: | ---: |
| 2026-11-21 | 1 | 5 | 1 |
| 2026-11-22 | 2 | 30 | 6 |

The first run has six deliveries: three accepted, one duplicate, one conflict, and one invalid. Running consume again reaches the same offset and leaves the views unchanged. Running the publisher again appends six deliveries and preserves the usage totals.

## Source and recovery

- Setup binds the PostgreSQL checkpoint to the Kafka topic ID and partition. Consume starts at the saved offset with Kafka offset commits disabled. If the binding or broker bounds change, the command stops and reports the failure.
- The first valid delivery for an event ID wins. Exact repeats after UTC normalization become duplicates. Changed values become conflicts. Invalid deliveries leave the ID available for a later valid record.
- Run `usage-stream consume` to follow the topic or use `--until-high-watermark` to process up to the offset captured at startup. Bounded consume defaults to 30 seconds. `--batch-size` defaults to 100 and accepts 1 through 1,000.
- Exit codes are 0 for success, 2 for invalid arguments, 3 for unsafe topic or offset, 4 for a stale checkpoint, 5 for an operation failure, and 6 for a bounded consume timeout. Following consume can be stopped with Ctrl+C.

Use one pipeline per database. Global options before the command set `--broker`, `--topic`, and `--pipeline`, or set `USAGE_BROKER`, `USAGE_TOPIC`, and `USAGE_PIPELINE`. To use a new database, initialize the schema and set `USAGE_INSTALLER_DSN` and `USAGE_CONSUMER_DSN` to the separate role connections. Use `USAGE_KAFKA_PORT` and `USAGE_POSTGRES_PORT` to change the Compose ports. Keep the supplied demo credentials on loopback.

For a clean rerun, stop the demo with `docker compose --env-file /dev/null -p api-usage-demo stop`. Use a fresh Compose project name for the next run. Compose creates that project's volumes and initializes a fresh database. The original project's data stays available. If both projects need to run at once, choose different ports and pass the new address to `USAGE_BROKER`.

## Verify

```bash
uv run --locked ruff check .
uv run --locked mypy src tests
uv run --locked pytest -q
```

Enable the integration suite against a disposable Compose project running these same images and role setup:

```bash
USAGE_TEST_INTEGRATION=1 uv run --locked pytest -q
```

Each integration case uses a new topic and initialized database. The suite includes a transaction marker that the real consumer must skip, read-only reporting checks, and an independent reference for the seeded fixture. Backfill and performance measurements are the next project units.
