# ADR 0002: Performance measurements

- Status: accepted
- Date: 2026-11-23

## Context

The README closes its Verify section by promising performance measurements as the next project unit. Operators need ballparks: how fast the consume path reconciles events, how quickly a dated archive can be republished, and what an interruption actually costs on resume. A timing assertion would be the wrong instrument: durations depend on the machine, and a threshold in the unit or integration suites would flake exactly where the project has spent most of its care.

At the same time, a benchmark that cannot fail is also a benchmark nobody trusts unless its method is written down: warmup bias, per-run state, run counts, and what a "run" measures all change the numbers.

## Decision

1. **An opt-in suite, gated at collection time.** The measurements live in `tests/perf` behind `USAGE_TEST_PERF=1`, mirrored from the integration suite's `USAGE_TEST_INTEGRATION` but enforced by a conftest `collect_ignore` rather than a fixture skip. Without the variable, a default run does not collect, import, or run any perf code; `tests/test_perf_gate.py` pins this from the never-gated unit suite by driving pytest in a subprocess over `tests/perf` (exit 5 and zero tests with the variable unset or `0`, successful collection with `1`).

2. **Durations are recorded, never asserted.** The perf tests assert structure only: exact CLI outputs and exit codes, stored offsets, delivery and fact row counts, published watermarks, per-family sizes in increasing order, positive durations, and the presence of a throughput column. No timing threshold exists anywhere in the repository, so no run can flake on duration and nothing gates CI. Timing values live in the perf reports, this ADR, and the README Performance section.

3. **Methodology.**
   - Every measured run gets a fresh topic and initialized database via the integration suite's `fresh_demo()` helper, so no run inherits checkpointed offsets, page state, or Kafka position.
   - One warmup per suite session publishes and consumes 1,000 events and its timings are discarded, to absorb process warmup, connection setup, and page-cold effects before any measured batch.
   - Every scenario and size is measured 3 times per suite session; the suite reports the median with the min and max of those runs, never a single run.
   - The whole suite was executed 3 times (2026-11-23) and the README table records the median of the three suite medians with the range across all nine runs.
   - Consume runs publish unique valid events (not timed) and then time `usage-stream consume --until-high-watermark --timeout-seconds 300` as a subprocess, wall clock from process start to exit.
   - Backfill runs time `usage-stream backfill` over every archive file of the size, so validation is inside the measured span because it is inside the same CLI invocation.
   - Recovery runs launch the real consumer in a child process paused at the `after_commit` seam, kill it with SIGKILL right after the first batch commit, then time a resumed `consume`; the deliberate barrier pause is excluded and the recovery total is compared against separate timed clean runs of the same fixture.

4. **Scenario set and configuration.**
   - Consume: 1,000, 10,000, and 50,000 events, at `--batch-size 1000`. Each consumed batch commits one PostgreSQL transaction and performs two broker guard lookups (watermark and topic identity) before it is accepted, which costs roughly half a second per batch; at the CLI's default batch of 100 the probe run needed more than 45 seconds for 10,000 events. The suite therefore measures the documented CLI maximum, which keeps the 10,000-event scenarios far below a minute and the 50,000-event scenario around 25 seconds.
   - Backfill: 10,000 and 100,000 archive rows. The CLI accepts at most 50,000 rows per dated archive, so the 100,000-row scenario publishes two 50,000-row archives (2026-11-21 and 2026-11-22) in one timed span; the per-file `Acknowledged N deliveries` outputs, the published watermark, and the first records' keys are asserted.
   - Recovery: a 2,000-event fixture, killed after the first 1,000-event batch commit, leaving a genuine 1,000-event remainder for the resumed process. The measured added wall time is the gap between the recovery medians and the clean-run medians (0.36 seconds), dominated by one extra process start plus the startup guard lookups the resumed process repeats, rather than by the number of events.

5. **Artifacts.** Each suite session writes a JSON report (measurements and per-scenario rows) to a temporary directory, overridable with `USAGE_TEST_PERF_OUT`; the numbers in the README Performance section and in `docs/benchmarks/throughput.svg` were recorded by hand from those reports. The SVG is static markup with a `role="img"` title and description carrying every value, so the data survives without the chart.

## Consequences

- Numbers are documentation, not gates. They were measured on one machine against the `api-usage-it` Compose stack (single Kafka broker and one PostgreSQL instance on loopback) and will differ on any other machine, especially under contention or with remote brokers.
- Measured medians (median of the three suite medians; range across all nine runs): consume 1,000 events 0.93 s (0.81 to 1.06) at 1,079 events/s; consume 10,000 events 4.95 s (4.86 to 5.05) at 2,022 events/s; consume 50,000 events 24.95 s (24.91 to 25.49) at 2,004 events/s; backfill 10,000 rows 0.70 s (0.68 to 0.74) at 14,251 rows/s; backfill 100,000 rows 2.01 s (1.98 to 2.09) at 49,680 rows/s; recovery clean run 1.38 s (1.31 to 1.62) at 1,453 events/s; recovery after a kill 1.73 s (1.69 to 1.89) at 1,154 events/s, an added 0.36 s against the clean median.
- The consume path is batch-latency bound, not throughput bound: at batch size 1,000 the per-event rate is nearly flat between 10,000 and 50,000 events, so the practical lever is the batch size, not the event volume. Publishing is roughly 25 times faster than consuming: backfill can deliver 100,000 rows in about 2 seconds, while consuming them reliably takes about 50.
- Before profiling or optimizing, rerun and compare: start the stack with `docker compose --env-file /dev/null -p api-usage-it up -d --wait`, then run `USAGE_TEST_PERF=1 uv run --locked pytest -q tests/perf` (about two minutes and 8 tests on the reference machine). Read the JSON report the run prints, and if a change is deliberate, amend the README table and the SVG by hand so the published numbers keep a single source.
- The default suites are unchanged: the unit and integration suites assert no durations, the unit suite additionally pins the perf gate, and CI runs the perf suite nowhere.
