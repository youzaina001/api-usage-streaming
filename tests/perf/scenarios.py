"""Scenario and measurement definitions for the opt-in performance suite.

The suite is documentation, not a gate: nothing in this module can fail a
default run. Two gate layers keep the test module out of every run without
USAGE_TEST_PERF=1: the collection-time collect_ignore in this directory's
conftest.py for directory runs, and the module-level skip in the test module
itself for files or test ids named directly on the command line. The
constants below are the recorded methodology (run count, warmup size, and
the measured sizes and batch size); ADR 0002 explains them.
"""
from dataclasses import dataclass
from typing import Final, Literal

Mode = Literal["consume", "backfill"]

#: Measured runs per scenario and size: the report shows their median and range.
RUNS: Final = 3
#: Warmup phase size: published and consumed once per suite session, timings discarded.
WARMUP_EVENTS: Final = 1_000
#: Consume scenarios: events published once per run, consumed to the high watermark.
CONSUME_SIZES: Final = (1_000, 10_000, 50_000)
#: Backfill scenarios: total archive rows published per run.
BACKFILL_SIZES: Final = (10_000, 100_000)
#: Rows per archive file: the CLI limit for one dated archive (docs/operations.md backfill rules).
BACKFILL_SPLIT: Final = 50_000
#: Recovery scenario: events published per run; the kill lands after the first batch.
RECOVERY_EVENTS: Final = 2_000
#: Consume batch size used by every measured run: the CLI maximum, not the default.
BATCH_SIZE: Final = 1_000


@dataclass(frozen=True)
class Scenario:
    """One measured workload: a row count and the CLI flow that drives it."""

    name: str
    events: int
    mode: Mode = "consume"


@dataclass(frozen=True)
class Measurement:
    """One timed run of one scenario, collected into a results list."""

    seconds: float
    events: int
    scenario: str
    run: int


def consume_scenarios() -> list[Scenario]:
    return [Scenario(f"consume-{events}", events) for events in CONSUME_SIZES]


def backfill_scenarios() -> list[Scenario]:
    return [Scenario(f"backfill-{events}", events, mode="backfill") for events in BACKFILL_SIZES]


RECOVERY_CLEAN: Final = Scenario("recovery-clean", RECOVERY_EVENTS)
RECOVERY_RESUME: Final = Scenario("recovery-resume", RECOVERY_EVENTS)
