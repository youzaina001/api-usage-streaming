"""Pin the perf collection gate: the suite collects only with USAGE_TEST_PERF=1.

These tests run in the base unit job, so any change that lets the perf module
collect (and eventually run timings) without the gate fails here first. The
gate is checked by collecting tests/perf in a subprocess with a controlled
environment: the same mechanism CI relies on, not an import of the gate.

pytest ignores collect_ignore for files named on the command line, so the pin
also collects the module and a single test by name, the way an IDE runner
would. The expected outcomes, asserted by exit code and collected ids:

- with the gate unset or 0, collecting tests/perf exits 5 (no tests
  collected by the conftest gate);
- with the gate unset or 0, collecting the named module exits 5 as well (the
  module-level skip reports no tests to collect);
- with the gate unset or 0, collecting a single test id exits 4, because
  pytest finds no collector once the module is skipped;
- with the gate open, all three paths collect successfully.
"""
import os
import subprocess
import sys
from pathlib import Path
from tempfile import gettempdir

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPORT_TEST = "tests/perf/test_perf_measurements.py::test_results_report"


def pytest_env(value: str | None) -> dict[str, str]:
    env = {name: setting for name, setting in os.environ.items() if name != "USAGE_TEST_PERF"}
    if value is not None:
        env["USAGE_TEST_PERF"] = value
    return env


def collect_target(target: str, value: str | None) -> subprocess.CompletedProcess[str]:
    """Collect `target` in a fresh pytest with USAGE_TEST_PERF set to `value`."""
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", target, "--collect-only"],
        cwd=ROOT, env=pytest_env(value), check=False, capture_output=True, text=True, timeout=120,
    )


def collected(result: subprocess.CompletedProcess[str]) -> list[str]:
    return [line for line in result.stdout.splitlines() if "::" in line]


def test_perf_tests_do_not_collect_without_the_gate() -> None:
    for value in (None, "0"):
        directory = collect_target("tests/perf", value)
        assert directory.returncode == 5, (value, directory.returncode, directory.stderr)
        assert collected(directory) == [], (value, directory.stdout)
        named = collect_target("tests/perf/test_perf_measurements.py", value)
        assert named.returncode == 5, (value, named.returncode, named.stderr)
        assert collected(named) == [], (value, named.stdout)
        single = collect_target(REPORT_TEST, value)
        assert single.returncode == 4, (value, single.returncode, single.stderr)
        assert collected(single) == [], (value, single.stdout)


def test_perf_tests_collect_with_the_gate_open() -> None:
    for target, expected_ids in (
        ("tests/perf", "directory"),
        ("tests/perf/test_perf_measurements.py", "named module"),
        (REPORT_TEST, "single test id"),
    ):
        result = collect_target(target, "1")
        assert result.returncode == 0, (expected_ids, result.returncode, result.stderr)
        ids = collected(result)
        assert any(id.endswith("::test_results_report") for id in ids), (expected_ids, ids)
    assert len(collected(collect_target(REPORT_TEST, "1"))) == 1


def test_perf_output_dir_falls_back_to_tempdir_when_env_unset_or_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.perf.harness import REPORT_ENV, PerfSuite

    default_dir = Path(gettempdir()) / "usage-stream-perf"
    monkeypatch.delenv(REPORT_ENV, raising=False)
    unset_suite = PerfSuite.new(tmp_path)
    assert unset_suite.results_path.parent == default_dir
    assert unset_suite.results_path.name.startswith("results-")
    assert unset_suite.results_path.suffix == ".json"

    monkeypatch.setenv(REPORT_ENV, "")
    empty_suite = PerfSuite.new(tmp_path)
    assert empty_suite.results_path.parent == default_dir

    custom_dir = tmp_path / "custom-perf-out"
    monkeypatch.setenv(REPORT_ENV, str(custom_dir))
    custom_suite = PerfSuite.new(tmp_path)
    assert custom_suite.results_path.parent == custom_dir
    assert custom_dir.is_dir()
