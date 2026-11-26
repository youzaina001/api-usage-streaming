# Operations

Recovery rules, reruns, dated backfill, and rebuilds for the pipeline. Commands run from the repository root; global options and exit codes are in the [Command reference](cli.md).

## Rerun the fixture

The first run has six deliveries: three accepted, one duplicate, one conflict, and one invalid. Running consume again reaches the same offset and leaves the views unchanged. Running the publisher again appends six deliveries and preserves the usage totals. Before publishing, `publish-fixture` checks that the topic exists with the expected configuration and exits 3 otherwise. If publication is uncertain, it exits 5 and reports the confirmed lower bound; rerun the whole fixture, or rebuild into a new topic and database if a conflicting row may have been published, because the first valid delivery for an event ID wins and a rerun cannot replace it.

## Source and recovery

- Setup binds the PostgreSQL checkpoint to the Kafka topic ID and partition. Consume starts at the saved offset with Kafka offset commits disabled. If the binding or broker bounds change, the command stops and reports the failure.
- The first valid delivery for an event ID wins. Exact repeats after UTC normalization become duplicates. Changed values become conflicts. Invalid deliveries leave the ID available for a later valid record.
- Run `usage-stream consume` to follow the topic or use `--until-high-watermark` to process up to the offset captured at startup; see [consume](cli.md#consume) for options and defaults.
- See [Exit codes](cli.md#exit-codes) for the full table. A bounded-consume timeout (exit 6) can leave committed deliveries and an advanced checkpoint, so a retry resumes from that checkpoint. A follow-mode run stops with Ctrl+C (exit 130).
- Each batch commits its facts, delivery outcomes, and next offset together. A consumer killed before the commit leaves the old offset and no rows from that batch. A consumer killed after the commit resumes after that batch. If the commit succeeds but the client sees an error, the command exits 5 and a restart continues from the saved offset. Two workers that fetched from the same offset cannot both commit: the second exits 4 without writing.

Use one pipeline per database. Global options and their environment variables are listed in the [Command reference](cli.md). To use a new database, initialize the schema and set `USAGE_INSTALLER_DSN` and `USAGE_CONSUMER_DSN` to the separate role connections. The rationale behind these rules is recorded in [ADR 0001](decisions/0001-recovery-and-backfill.md).

### Clean rerun

From the repository root, use two free loopback ports. Compose initializes the `usage` database on a fresh volume for the new project. Setup creates the topic selected by `USAGE_TOPIC`.

```bash
(
  set -e
  project="api-usage-rerun-$(date -u +%Y%m%d%H%M%S)"
  export USAGE_TOPIC="$project"
  export USAGE_KAFKA_PORT=29092 USAGE_POSTGRES_PORT=25432
  unset USAGE_BROKER USAGE_INSTALLER_DSN USAGE_CONSUMER_DSN
  trap 'docker compose --env-file /dev/null -p "$project" stop' EXIT
  docker compose --env-file /dev/null -p "$project" up -d --wait
  uv run --locked usage-stream setup
  uv run --locked usage-stream publish-fixture
  uv run --locked usage-stream consume --until-high-watermark --timeout-seconds 30
)
```

The rerun reports `Next offset 6` and stops its Compose project. Its volumes remain available for inspection.

## Backfill a dated archive

`usage-stream backfill --file PATH --manifest PATH` publishes a JSONL file of usage events for one UTC day. The manifest is a JSON object with exactly `day`, `sha256` (lowercase hex of all file bytes), and `row_count`:

```bash
uv run --locked usage-stream backfill \
  --file tests/fixtures/archive-2026-11-21.jsonl \
  --manifest tests/fixtures/archive-2026-11-21.manifest.json
uv run --locked usage-stream consume --until-high-watermark
```

Validation happens before any record is sent, and any validation failure exits 2 and publishes nothing. The file can be at most 16 MiB and 50,000 rows, with LF row separators, an optional final LF, and no blank rows. Each row must be a valid event of at most 16,384 bytes whose UTC event time falls on the manifest day. Rows that repeat an event ID must carry the same fact after UTC normalization; a conflicting repeat is rejected too, so rerunning a file can never change which conflicting record would win. The command also checks that the topic exists with the expected configuration and exits 3 otherwise. The command publishes the validated bytes in file order with the event IDs as keys, so a later change to the file cannot reach Kafka.

After the demo above, backfilling the fixture archive adds a new 2026-11-21 18:00 UTC event (7 input and 3 output tokens), so 2026-11-21 becomes 2 requests, 12 input tokens, and 4 output tokens. The archive's other row repeats the seeded 2026-11-21 23:30 UTC event (5 input and 1 output token) and counts as a duplicate. Running the same backfill again appends two more duplicates and leaves the totals unchanged. Kafka stamps each record with its append time for retention, while the event time decides the usage day.

If publication is uncertain, the command exits 5 and reports how many rows Kafka confirmed. That number is a lower bound, because a row can reach Kafka without its acknowledgement reaching the client. Rerun the whole file. Rows already present become duplicates and the missing rows are added.

### Rebuild after a replaced topic or expired offset

When consume exits 3 because the topic changed or retention passed the saved offset, keep the existing database for inspection and rebuild into a new topic and database. Initialize the schema in the new database, then pass explicit options before each command:

```bash
uv run --locked usage-stream --topic api-usage-rebuild \
  --installer-dsn "$REBUILD_INSTALLER_DSN" --consumer-dsn "$REBUILD_CONSUMER_DSN" setup
uv run --locked usage-stream --topic api-usage-rebuild \
  --installer-dsn "$REBUILD_INSTALLER_DSN" --consumer-dsn "$REBUILD_CONSUMER_DSN" \
  backfill --file ARCHIVE --manifest MANIFEST
uv run --locked usage-stream --topic api-usage-rebuild \
  --installer-dsn "$REBUILD_INSTALLER_DSN" --consumer-dsn "$REBUILD_CONSUMER_DSN" \
  consume --until-high-watermark
```

The rebuilt totals describe only the inputs replayed into the new database. Deliveries that exist in no archive cannot be recovered. Replay archives in their original order, because the first valid record for an event ID wins and a different order of conflicting records can change the winning fact. Record the order and the manifest checksums used for each rebuild.
