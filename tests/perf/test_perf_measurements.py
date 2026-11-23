"""Opt-in performance measurements; collected only with USAGE_TEST_PERF=1.

The suite is documentation, not a gate: it drives the same Compose stack as
the integration suite and records wall times for the reports under docs/.
Every assertion here is structural (fixture counts, size ordering, positive
durations, a throughput column); timing thresholds never appear, so this
module cannot flake or gate CI, and the collection gate in conftest.py
keeps it out of every default run anyway.

Run it with the stack up:

    docker compose --env-file /dev/null -p api-usage-it up -d --wait
    USAGE_TEST_PERF=1 uv run --locked pytest -q tests/perf -s

Each measured run gets a fresh topic and initialized database (the
integration fresh_demo() helper), after one per-session warmup whose
timings are discarded. Run everything in tests/perf together: the final
report test reads what the earlier tests recorded.
"""
import pytest

from .harness import PerfSuite
from .scenarios import (
    BACKFILL_SIZES,
    CONSUME_SIZES,
    RECOVERY_CLEAN,
    RECOVERY_EVENTS,
    RECOVERY_RESUME,
    RUNS,
    Measurement,
    Scenario,
    backfill_scenarios,
    consume_scenarios,
)


@pytest.fixture(scope="session")
def suite(tmp_path_factory: pytest.TempPathFactory) -> PerfSuite:
    instance = PerfSuite.new(archive_root=tmp_path_factory.mktemp("perf-archives"))
    instance.warmup()
    return instance


def test_scenario_and_measurement_are_pure_construction() -> None:
    scenario = Scenario(name="fixture-100", events=100)
    assert scenario.name == "fixture-100"
    assert scenario.events == 100
    assert scenario.mode == "consume"
    measurement = Measurement(seconds=1.5, events=scenario.events, scenario=scenario.name, run=1)
    assert (measurement.seconds, measurement.events, measurement.scenario, measurement.run) == (
        1.5, 100, "fixture-100", 1,
    )


@pytest.mark.parametrize(
    "scenario", consume_scenarios(), ids=[scenario.name for scenario in consume_scenarios()],
)
def test_consume_scenario(suite: PerfSuite, scenario: Scenario) -> None:
    measurements = [suite.consume_run(scenario, run) for run in range(1, RUNS + 1)]
    suite.record(scenario, measurements)


@pytest.mark.parametrize(
    "scenario", backfill_scenarios(), ids=[scenario.name for scenario in backfill_scenarios()],
)
def test_backfill_scenario(suite: PerfSuite, scenario: Scenario) -> None:
    measurements = [suite.backfill_run(scenario, run) for run in range(1, RUNS + 1)]
    suite.record(scenario, measurements)


def test_recovery_scenario(suite: PerfSuite) -> None:
    clean = [suite.recovery_clean_run(run) for run in range(1, RUNS + 1)]
    resume = [suite.recovery_resume_run(run) for run in range(1, RUNS + 1)]
    suite.record(RECOVERY_CLEAN, clean)
    suite.record(RECOVERY_RESUME, resume)


def test_results_report(suite: PerfSuite) -> None:
    rows = suite.rows
    assert [row.events for row in rows if row.scenario.startswith("consume-")] == list(
        CONSUME_SIZES,
    )
    assert [row.events for row in rows if row.scenario.startswith("backfill-")] == list(
        BACKFILL_SIZES,
    )
    recovery = [row for row in rows if row.scenario in (RECOVERY_CLEAN.name, RECOVERY_RESUME.name)]
    assert [row.events for row in recovery] == [RECOVERY_EVENTS, RECOVERY_EVENTS]
    for name in (row.scenario for row in rows):
        measured = [m for m in suite.results if m.scenario == name]
        assert len(measured) == RUNS, name
        assert [m.run for m in measured] == list(range(1, RUNS + 1)), name
    for row in rows:
        assert row.events > 0, row
        assert row.min_seconds <= row.median_seconds <= row.max_seconds, row
        assert row.events_per_second > 0, row
    suite.write_report()
    suite.print_report()
