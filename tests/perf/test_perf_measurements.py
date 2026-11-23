"""Opt-in performance measurements; collected only with USAGE_TEST_PERF=1.

The suite records wall-clock timings for the reports under docs/ and asserts
structural invariants only. No timing threshold lives here, so nothing in this
module can gate CI; the collection gate in this directory's conftest.py keeps
the module out of every default run.
"""
from .scenarios import Measurement, Scenario


def test_scenario_and_measurement_are_pure_construction() -> None:
    scenario = Scenario(name="fixture-100", events=100)
    assert scenario.name == "fixture-100"
    assert scenario.events == 100
    assert scenario.mode == "consume"
    measurement = Measurement(
        seconds=1.5, events=scenario.events, scenario=scenario.name, run=1,
    )
    assert (measurement.seconds, measurement.events, measurement.scenario, measurement.run) == (
        1.5, 100, "fixture-100", 1,
    )
