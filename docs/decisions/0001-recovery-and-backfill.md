# ADR 0001: Recovery from interruption and backfill of dated archives

- Status: accepted
- Date: 2026-11-23

## Context

The pipeline reconciles API usage events from one Kafka partition into daily request and token totals in PostgreSQL. Operators need safe answers to three questions:

1. What happens when a consumer or publisher is interrupted in the middle of its work?
2. Can a dated archive be replayed into the topic to rebuild usage?
3. What happens when two records for the same event ID carry different facts?

Naive answers are dangerous: at-least-once delivery can double-count events, a rerun can silently change which conflicting record wins, and publishing into a replaced topic can split the stream across two sources of truth.

## Decision

1. **Single-transaction batches.** Each batch commits the accepted facts, one outcome per delivered record, and the next Kafka offset in a single PostgreSQL transaction. Kafka offset commits stay disabled; the database checkpoint is the only cursor. A consumer killed before the commit leaves the old offset and no rows from that batch; killed after, it resumes after that batch. Two workers that fetched from the same offset cannot both commit: the second fails with a stale checkpoint (exit 4) and writes nothing.

2. **Checkpoint bound to the source.** Setup binds the checkpoint to the Kafka topic ID and partition. If the binding or the broker bounds change (a replaced topic, or retention that has passed the saved offset), the command refuses with exit 3 instead of reading from an unsafe position. A `consume` run without a checkpoint also exits 3 and tells the operator to run `setup` first.

3. **First valid delivery wins; idempotent publish.** The first valid delivery for an event ID wins. Exact repeats after UTC normalization become duplicates; changed values become conflicts; invalid deliveries leave the ID available for a later valid record. Publishers key records by event ID and send them in a fixed order: `backfill` sends the validated file bytes in file order, and `publish-fixture` sends the bundled fixture. Rerunning either publisher only re-sends the same records in the same order, so it can never change which conflicting record would win. As an extra guard, `backfill` rejects an archive whose own rows repeat an event ID with a conflicting fact at validation time (exit 2).

4. **Publishers pre-check the topic.** `publish-fixture` and `backfill` verify that the topic exists with the expected configuration before sending anything, and exit 3 otherwise, rather than waiting for a broker error mid-publish. The pre-check does not compare topic identity with the checkpoint; a replaced topic is caught later by `setup` and `consume` (Decision 2). If publication is uncertain (a row may have reached Kafka without its acknowledgement reaching the client), the command exits 5 and prints the confirmed count, which is a lower bound. An interrupted publisher exits 130 with no count at all, so after Ctrl+C the same rerun rule applies even though no lower bound is printed.

5. **Exit-code contract.** 0 success; 2 invalid arguments or a rejected archive; 3 unsafe or missing source; 4 stale checkpoint; 5 operation failure or uncertain publication; 6 consume deadline reached; 130 interrupted with Ctrl+C. The [README exit-code table](../../README.md#exit-codes) is the canonical reference.

6. **Private recovery hooks for tests.** `_seams.py` exposes `fetched`, `before_commit`, and `after_commit` synchronization points that only tests install, so recovery races can be exercised deterministically in child processes without changing production behavior.

## Consequences

- Effectively once per delivery: Kafka delivery is at-least-once, but each offset is recorded once in the checkpoint and redelivered rows become duplicates, so nothing double-counts. A racing second worker is fenced by the checkpoint and writes nothing.
- Rerunning a publisher never changes which conflicting record wins, because it re-sends the same records in the same order; rows already in Kafka become duplicates and rows that never landed are added (that is the recovery mechanism after an interrupted publish). One hazard remains, and it applies to `publish-fixture`: the fixture deliberately contains a conflicting row after its original for the same event ID, so if the original's delivery fails while the later conflicting row succeeds, the conflict wins and a rerun cannot replace it. In that case, rebuild into a new topic and database.
- A replaced topic or an expired offset requires a full rebuild into a new topic and database; deliveries that exist in no archive cannot be recovered. Replay archives in their original order, because a different order of conflicting records can change the winning fact, and record the order and manifest checksums used for each rebuild.
- The exit-code contract tells scripts what to do next: 2 means fix the input; 4 means another worker owns the offset, so stop or retry later; 5 and 6 mean rerun the same command (except that an uncertain `publish-fixture` may need a rebuild, above); 3 means inspect the source, running `setup` when the topic or checkpoint is missing and rebuilding when the topic identity changed or retention passed the saved offset.
- Broker metadata for a freshly created topic can lag by a moment, so each source-check broker lookup retries unknown-topic and unknown-partition errors (`UNKNOWN_TOPIC_OR_PART` and related codes) for up to five seconds before exiting 3. A topic that stays missing still fails closed, and the retry never applies to offset-range or identity failures.
