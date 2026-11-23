"""Scenario and measurement definitions for the opt-in performance suite.

The suite is documentation, not a gate: nothing in this module can fail a
default run, because the collection gate in this directory's conftest.py
keeps the test module out of every run without USAGE_TEST_PERF=1.
"""
from dataclasses import dataclass
from typing import Literal

Mode = Literal["consume", "backfill"]


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
