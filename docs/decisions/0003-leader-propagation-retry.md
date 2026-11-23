# ADR 0003: Retrying leadership lag in the topic propagation window

- Status: accepted
- Date: 2026-11-23

## Context

A source check right after topic creation races the broker's KRaft metadata propagation, so `consumer.py` retries the guard's broker lookups for up to five seconds before failing closed with exit 3 (ADR 0001). The demo fixtures create a fresh topic per test and `setup` immediately queries its watermarks, which makes integration CI the tripwire for this window. Two CI runs recorded the race, each with a different error code:

1. Run 45155856074 on the PR #6 head failed 2 tests in that head's 130-test CI job with `UNKNOWN_TOPIC_OR_PART`; the guard classified them as a missing topic and the run failed closed with exit 3 (unsafe or missing source), as commit e5fb51c on the #7 branch records. The parked PR #7 added the propagation retry that absorbs this flavor.
2. Run 45178752855 on the PR #8 head f661aeb failed 2 of the 140 tests in its CI job (138 passed; the job runs the whole suite with `USAGE_TEST_INTEGRATION=1`, which on that head is 92 unit plus 48 integration, and both failures were integration tests), both at `demo.cli("setup")`, with `KafkaError{code=NOT_LEADER_FOR_PARTITION,val=6,str="Failed to get watermark offsets: Broker: Not leader for partition"}` and exit 5 (`Kafka operation failed`), in `tests/integration/test_backfill.py::test_partial_publication_is_retryable[lost-ack]` and `tests/integration/test_ingestion.py::test_offsets_need_not_be_contiguous`.

The mechanism: `_check_broker_error` classifies codes in `PROPAGATION_ERRORS` as `TopicUnavailable`, which `_retry_propagation` absorbs up to the five-second deadline; codes in `SOURCE_ERRORS` outside that set fail fast as a plain `SourceGuardError`; codes in neither set fall through, and the raw `KafkaException` escapes to the CLI, which prints `Kafka operation failed` and exits 5. `NOT_LEADER_FOR_PARTITION` (value 6) and `LEADER_NOT_AVAILABLE` (value 5) were in neither set, so the same post-creation race escaped unabsorbed through a second flavor: leadership lag, where the partition exists but its leader has not propagated yet. The PR #8 delta touches no broker code, so this is a latent base defect, and the fix lands as a stacked PR on top of it, the same treatment the first flavor received.

## Decision

1. **Widen both classification sets with the two leadership codes.** Add `KafkaError.NOT_LEADER_FOR_PARTITION` and `KafkaError.LEADER_NOT_AVAILABLE` to both `SOURCE_ERRORS` and `PROPAGATION_ERRORS` in `src/usage_stream/consumer.py`, so they classify as retriable `TopicUnavailable` and ride the same propagation retry as the unknown-topic codes. `PROPAGATION_TIMEOUT`, `PROPAGATION_PAUSE`, and the retry structure are untouched.

2. **Why retrying these errors is safe.** Every operation the retry wraps is an idempotent broker read: describe topics, describe configs, and the watermark query. Retrying a read can never write twice, and both codes are metadata-only conditions, the leadership analogue of unknown-topic: the answer exists and arrives once leadership settles. Offset-range failures (`OFFSET_OUT_OF_RANGE`, `_AUTO_OFFSET_RESET`) and topic-identity mismatches remain non-retriable and fail fast exactly as before.

3. **Fail-closed stays fail-closed.** A leadership error that persists past the five-second deadline keeps raising `TopicUnavailable` until the retry gives up, and the guard still exits 3 (unsafe or missing source). Only the propagation race is absorbed; a genuinely broken cluster still stops the pipeline at the source guard.

## Consequences

- Both recorded flavors of the post-creation race, unknown-topic and leadership lag, now fall inside one retry window, so neither can escape a `setup` guard check as an unclassified exit 5.
- The retry never widens past metadata lag: offset-range and topic-identity failures still fail fast, so the guard keeps failing closed with exit 3 whenever the source is genuinely unsafe.
- This ADR extends ADR 0001's closing consequence, which said each source-check broker lookup retries unknown-topic and unknown-partition errors; the retried set now also names `NOT_LEADER_FOR_PARTITION` and `LEADER_NOT_AVAILABLE`, still for up to five seconds and still failing closed.
- `tests/test_consumer.py` pins the widening at three levels: both codes classify as `TopicUnavailable`, the `check()` watermark lookup absorbs a leadership failure and succeeds on the third lookup, and a leadership error that never settles still raises `TopicUnavailable` at the deadline, mirroring the topic-missing deadline pin on the same fake clock.
