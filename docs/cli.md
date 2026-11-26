# Command reference

All commands share global options given before the command name:

```text
usage-stream [--broker HOST:PORT] [--topic NAME] [--pipeline NAME]
             [--installer-dsn DSN] [--consumer-dsn DSN] COMMAND ...
```

| Option | Environment variable | Default |
| --- | --- | --- |
| `--broker` | `USAGE_BROKER` | `127.0.0.1:19092` (the port comes from `USAGE_KAFKA_PORT` when set) |
| `--topic` | `USAGE_TOPIC` | `api-usage` |
| `--pipeline` | `USAGE_PIPELINE` | `usage` |
| `--installer-dsn` | `USAGE_INSTALLER_DSN` | loopback `usage` database on `USAGE_POSTGRES_PORT` (default `15432`) as the `installer` role |
| `--consumer-dsn` | `USAGE_CONSUMER_DSN` | loopback `usage` database on `USAGE_POSTGRES_PORT` (default `15432`) as the `usage_consumer` role |

`USAGE_KAFKA_PORT` and `USAGE_POSTGRES_PORT` change both the Compose ports and the CLI's loopback defaults. Broker selection takes the first available value from `--broker`, `USAGE_BROKER`, and the loopback default. `setup` connects to PostgreSQL with the installer DSN and `consume` with the consumer DSN; `publish-fixture` and `backfill` talk only to Kafka. Keep the supplied demo credentials on loopback.

## setup

```bash
uv run --locked usage-stream setup
```

On a fresh database, creates the topic when missing. It then checks the topic configuration and binds the PostgreSQL checkpoint to the topic ID and partition; if an existing checkpoint points at a missing, replaced, or truncated topic, it exits 3. Running it again preserves a matching checkpoint.

## publish-fixture

```bash
uv run --locked usage-stream publish-fixture
```

Publishes the seeded fixture deliveries with their event IDs as keys. Requires the topic to exist with the expected configuration, and exits 3 otherwise. If publication is uncertain, exits 5 and prints the confirmed lower bound; see [Rerun the fixture](operations.md#rerun-the-fixture) for the rerun rule.

## backfill

```bash
uv run --locked usage-stream backfill --file PATH --manifest PATH
```

Validates the JSONL archive and its manifest, then publishes the validated bytes in file order. A validation failure exits 2 and publishes nothing; a missing or incompatible topic exits 3 before any record is sent. See [Backfill a dated archive](operations.md#backfill-a-dated-archive).

## consume

```text
uv run --locked usage-stream consume [--until-high-watermark]
                                     [--timeout-seconds SECONDS] [--batch-size N]
```

Follows the topic, or with `--until-high-watermark` processes up to the offset captured at startup. `--batch-size` defaults to 100 and accepts 1 through 1,000; `--timeout-seconds` must be finite and positive. A bounded run defaults to 30 seconds when `--timeout-seconds` is omitted, and `--timeout-seconds` also caps a follow-mode run, which then exits 6 at the deadline. Invalid values exit 2. Stop a follow-mode run early with Ctrl+C (exit 130).

## Exit codes

| Code | Meaning | Raised by |
| ---: | --- | --- |
| 0 | Success | all commands |
| 2 | Invalid arguments, or an archive or manifest that cannot be read or fails validation (nothing is published) | all commands; archive/manifest rejection only `backfill` |
| 3 | Unsafe or missing source: a missing or incompatible topic, a missing checkpoint (run `setup` first), a changed topic identity, or a saved offset outside the broker bounds | all commands |
| 4 | Stale checkpoint: another worker committed from the same offset first and wrote the batch | `consume` |
| 5 | Operation failure (Kafka, PostgreSQL, or a driver timeout), or uncertain publication with the confirmed lower bound printed | all commands |
| 6 | Consume deadline reached (`--timeout-seconds`, bounded or follow mode); committed batches and the checkpoint are kept, so a retry resumes | `consume` |
| 130 | Interrupted with Ctrl+C; an interrupted publisher may already have sent part of its rows | all commands |

A topic created milliseconds earlier may not have propagated its topic metadata or partition leadership to the broker yet, so each source-check broker lookup retries those transient propagation errors for up to five seconds before exiting 3 ([ADR 0003](decisions/0003-leader-propagation-retry.md)); a topic or leader that stays unavailable still fails closed.
