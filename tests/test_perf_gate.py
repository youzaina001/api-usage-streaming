"""Pin the perf collection gate: the suite collects only with USAGE_TEST_PERF=1.

These tests run in the base unit job, so any change that lets the perf module
collect (and eventually run timings) without the gate fails here first. The
gate is checked by collecting tests/perf in a subprocess with a controlled
environment: the same mechanism CI relies on, not an import of the gate.
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def collect_perf(value: str | None) -> subprocess.CompletedProcess[str]:
    """Collect tests/perf in a fresh pytest with USAGE_TEST_PERF set to `value`."""
    env = {name: setting for name, setting in os.environ.items() if name != "USAGE_TEST_PERF"}
    if value is not None:
        env["USAGE_TEST_PERF"] = value
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "tests/perf", "--collect-only"],
        cwd=ROOT, env=env, check=False, capture_output=True, text=True, timeout=120,
    )


def collected(result: subprocess.CompletedProcess[str]) -> list[str]:
    return [line for line in result.stdout.splitlines() if "::" in line]


def test_perf_tests_do_not_collect_without_the_gate() -> None:
    for value in (None, "0"):
        result = collect_perf(value)
        assert result.returncode == 5, (value, result.returncode, result.stdout, result.stderr)
        assert collected(result) == [], (value, result.stdout)


def test_perf_tests_collect_with_the_gate_open() -> None:
    result = collect_perf("1")
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
    assert collected(result), (result.stdout, result.stderr)
