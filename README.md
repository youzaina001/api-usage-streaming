# API Usage Streaming Pipeline

Reconcile synthetic API usage events from one Kafka partition into daily request and token totals in PostgreSQL. Each batch commits accepted facts, one outcome per delivered record, and the next Kafka offset in a single transaction.

| Measured on one machine | Result |
| --- | ---: |
| Consume 50,000 events to the high watermark, batch 1,000 | 2,004 events/s |
| Backfill: validate and publish 100,000 archived rows | 49,680 rows/s |
| Resume 2,000 events after a kill at the first commit | about 0.36 s over a clean run |

The numbers come from an opt-in suite and never gate CI. [Performance](docs/performance.md) has the method, the full table, and the chart.

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

The first run has six deliveries: three accepted, one duplicate, one conflict, and one invalid. Running consume again reaches the same offset and leaves the views unchanged. [Operations](docs/operations.md#rerun-the-fixture) covers reruns and uncertain publication.

Stop the demo after checking the results:

```bash
docker compose --env-file /dev/null -p api-usage-demo stop
```

## Guarantees

- Each batch commits its facts, delivery outcomes, and next offset in one PostgreSQL transaction. A consumer killed before the commit leaves the old offset and no rows from that batch; one killed after it resumes after that batch.
- The checkpoint is bound to the Kafka topic ID and partition. A missing, replaced, or truncated topic stops the command with exit 3 instead of restarting silently.
- The first valid delivery for an event ID wins. Exact repeats become duplicates and changed values become conflicts.
- Two workers that fetched from the same offset cannot both commit: the second exits 4 without writing.
- Backfill validates the whole archive and its manifest before it publishes any record.

## Documentation

| Page | Contents |
| --- | --- |
| [Command reference](docs/cli.md) | Global options, the four commands, and exit codes |
| [Operations](docs/operations.md) | Recovery rules, clean reruns, dated backfill, and rebuilds |
| [Testing and CI](docs/testing.md) | Lint, types, unit and integration suites, and the CI jobs |
| [Performance](docs/performance.md) | Measurement method, results, and the throughput chart |
| [Decisions](docs/decisions/) | ADR 0001 recovery and backfill, ADR 0002 performance measurements, ADR 0003 leader propagation retry |
