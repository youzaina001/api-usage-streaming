"""Timing harness for the opt-in performance suite (USAGE_TEST_PERF=1).

Mirrors the integration suite's machine (fresh_demo, Child, publish) and adds
wall-clock timing around CLI subprocesses plus the results recorder. Timings
exist only to be recorded for the reports under docs/; every check here is
structural: exit codes, exact command outputs, row and fact counts, and
ordering. No duration threshold is ever asserted, so no run can flake on a
timing and nothing here gates CI: the conftest collect_ignore gate for
directory runs plus the module-level skip in the test module keep the suite
out of every run without USAGE_TEST_PERF=1 (tests/test_perf_gate.py pins both
layers from the never-gated unit suite).
"""
import json
import os
import signal
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from tempfile import gettempdir
from time import monotonic
from typing import Self
from uuid import NAMESPACE_URL, uuid5

from tests.archive_builder import event, manifest, row, write_archive
from tests.integration.conftest import Demo, fresh_demo
from tests.integration.helpers import Child, read_records, state, watermarks
from tests.integration.test_ingestion import publish

from .scenarios import (
    BACKFILL_SPLIT,
    BATCH_SIZE,
    RECOVERY_CLEAN,
    RECOVERY_EVENTS,
    RECOVERY_RESUME,
    RUNS,
    WARMUP_EVENTS,
    Measurement,
    Scenario,
)

DAY = "2026-11-21"
ARCHIVE_DAYS = ("2026-11-21", "2026-11-22")
CLI_DEADLINE_SECONDS = "300"
PROCESS_TIMEOUT_SECONDS = 600
REPORT_ENV = "USAGE_TEST_PERF_OUT"


def event_row(tag: str, index: int, day: str = DAY) -> tuple[bytes, bytes]:
    """One unique valid event: the event UUID as key and the JSON row as payload."""
    event_id = str(uuid5(NAMESPACE_URL, f"perf/{tag}/{index}"))
    return event_id.encode(), row(event(event_id, f"{day}T12:00:00Z", 1, 1))


def event_rows(tag: str, count: int, day: str = DAY) -> list[tuple[bytes, bytes]]:
    return [event_row(tag, index, day) for index in range(count)]


@dataclass(frozen=True)
class ArchiveFile:
    archive: Path
    manifest: Path
    rows: int
    tag: str
    day: str


@dataclass(frozen=True)
class ScenarioRow:
    """The throughput column of the report: one line per scenario and size."""

    scenario: str
    mode: str
    events: int
    median_seconds: float
    min_seconds: float
    max_seconds: float
    events_per_second: float


def _consume_args() -> tuple[str, ...]:
    return (
        "consume", "--until-high-watermark",
        "--batch-size", str(BATCH_SIZE), "--timeout-seconds", CLI_DEADLINE_SECONDS,
    )


class PerfSuite:
    """Records wall times per scenario and run; checks structure only."""

    def __init__(self, archive_root: Path, results_path: Path) -> None:
        self.archive_root = archive_root
        self.results_path = results_path
        self.started_at = datetime.now(UTC)
        self.results: list[Measurement] = []
        self.rows: list[ScenarioRow] = []
        self._archives: dict[int, tuple[ArchiveFile, ...]] = {}

    @classmethod
    def new(cls, archive_root: Path) -> Self:
        out = Path(os.environ.get(REPORT_ENV) or str(Path(gettempdir()) / "usage-stream-perf"))
        out.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return cls(archive_root, out / f"results-{stamp}.json")

    def run_cli(self, demo: Demo, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(Path(sys.executable).with_name("usage-stream")), *args],
            env=os.environ | demo.cli_env(),
            check=False, capture_output=True, text=True, timeout=PROCESS_TIMEOUT_SECONDS,
        )

    def timed_cli(self, demo: Demo, *args: str) -> tuple[float, str]:
        start = monotonic()
        result = self.run_cli(demo, *args)
        span = monotonic() - start
        assert result.returncode == 0, ("cli failed", args, result.returncode, result.stderr)
        return span, result.stdout.strip()

    @staticmethod
    def publish_events(demo: Demo, tag: str, count: int, day: str = DAY) -> None:
        publish(demo, event_rows(tag, count, day))

    def warmup(self) -> None:
        demo = fresh_demo()
        self.timed_cli(demo, "setup")
        self.publish_events(demo, "warmup", WARMUP_EVENTS)
        span, out = self.timed_cli(demo, *_consume_args())
        assert out == f"Next offset {WARMUP_EVENTS}"
        assert state(demo) == (WARMUP_EVENTS, WARMUP_EVENTS, WARMUP_EVENTS)
        print(f"warmup: published and consumed {WARMUP_EVENTS} events in {span:.2f}s (discarded)")

    def consume_run(self, scenario: Scenario, run: int) -> Measurement:
        demo = fresh_demo()
        self.timed_cli(demo, "setup")
        self.publish_events(demo, f"{scenario.name}-r{run}", scenario.events)
        span, out = self.timed_cli(demo, *_consume_args())
        assert out == f"Next offset {scenario.events}"
        assert state(demo) == (scenario.events, scenario.events, scenario.events)
        return Measurement(seconds=span, events=scenario.events, scenario=scenario.name, run=run)

    def archives_for(self, rows_in_scenario: int) -> tuple[ArchiveFile, ...]:
        """Bulk archive files for a backfill scenario, cached per suite session.

        The CLI accepts at most 50,000 rows per dated archive, so a 100,000-row
        scenario is published as two 50,000-row archives on two UTC days.
        """
        cached = self._archives.get(rows_in_scenario)
        if cached is not None:
            return cached
        files: list[ArchiveFile] = []
        for chunk_index, offset in enumerate(range(0, rows_in_scenario, BACKFILL_SPLIT)):
            chunk = min(BACKFILL_SPLIT, rows_in_scenario - offset)
            tag = f"backfill-{rows_in_scenario}-c{chunk_index}"
            day = ARCHIVE_DAYS[chunk_index % len(ARCHIVE_DAYS)]
            data = b"".join(payload + b"\n" for _, payload in event_rows(tag, chunk, day))
            archive, manifest_path = write_archive(
                self.archive_root, data, manifest(data, chunk, day), name=tag,
            )
            files.append(ArchiveFile(archive, manifest_path, chunk, tag, day))
        recorded = tuple(files)
        self._archives[rows_in_scenario] = recorded
        return recorded

    def backfill_run(self, scenario: Scenario, run: int) -> Measurement:
        demo = fresh_demo()
        self.timed_cli(demo, "setup")
        files = self.archives_for(scenario.events)
        total = 0.0
        for archive_file in files:
            span, out = self.timed_cli(
                demo, "backfill",
                "--file", str(archive_file.archive),
                "--manifest", str(archive_file.manifest),
            )
            assert out == f"Acknowledged {archive_file.rows} deliveries"
            assert span > 0
            total += span
        assert watermarks(demo) == (0, scenario.events)
        header_file = files[0]
        header = read_records(demo, 0, 3)
        assert [record.key() for record in header] == [
            key for key, _ in event_rows(header_file.tag, 3, header_file.day)
        ]
        return Measurement(seconds=total, events=scenario.events, scenario=scenario.name, run=run)

    def recovery_clean_run(self, run: int) -> Measurement:
        demo = fresh_demo()
        self.timed_cli(demo, "setup")
        self.publish_events(demo, f"recovery-clean-r{run}", RECOVERY_EVENTS)
        span, out = self.timed_cli(demo, *_consume_args())
        assert out == f"Next offset {RECOVERY_EVENTS}"
        assert state(demo) == (RECOVERY_EVENTS, RECOVERY_EVENTS, RECOVERY_EVENTS)
        return Measurement(seconds=span, events=RECOVERY_EVENTS, scenario=RECOVERY_CLEAN.name, run=run)

    def recovery_resume_run(self, run: int) -> Measurement:
        demo = fresh_demo()
        self.timed_cli(demo, "setup")
        self.publish_events(demo, f"recovery-resume-r{run}", RECOVERY_EVENTS)
        launched = monotonic()
        owned: list[subprocess.Popen[str]] = []
        barrier = self.archive_root / f"recovery-r{run}-barrier"
        child = Child(demo, owned, barrier, ["after_commit"], *_consume_args())
        try:
            offset = child.reached("after_commit")
            paused_at_marker = monotonic()
            child.process.send_signal(signal.SIGKILL)
            killed_at = monotonic()
            code, child_out, child_err = child.finish()
            assert offset == BATCH_SIZE, (offset, child_out, child_err)
            assert code == -signal.SIGKILL, (code, child_out, child_err)
            # The deliberate pause (marker observed to kill) is test machinery, not work.
            child_useful = paused_at_marker - launched
            pause_window = killed_at - paused_at_marker
            assert state(demo) == (BATCH_SIZE, BATCH_SIZE, BATCH_SIZE)
        finally:
            if child.process.poll() is None:
                child.process.kill()
                child.process.communicate(timeout=10)
        resume_span, out = self.timed_cli(demo, *_consume_args())
        assert out == f"Next offset {RECOVERY_EVENTS}"
        assert state(demo) == (RECOVERY_EVENTS, RECOVERY_EVENTS, RECOVERY_EVENTS)
        print(
            f"recovery run {run}: killed after the first commit, pause window "
            f"{pause_window * 1000:.0f}ms excluded, "
            f"child {child_useful:.2f}s + resume {resume_span:.2f}s",
        )
        return Measurement(
            seconds=child_useful + resume_span,
            events=RECOVERY_EVENTS, scenario=RECOVERY_RESUME.name, run=run,
        )

    def record(self, scenario: Scenario, measurements: list[Measurement]) -> None:
        assert len(measurements) == RUNS, scenario.name
        for measurement, run in zip(measurements, range(1, RUNS + 1), strict=True):
            assert measurement.scenario == scenario.name
            assert measurement.events == scenario.events
            assert measurement.run == run
            assert measurement.seconds > 0, measurement
        ordered = sorted(measurement.seconds for measurement in measurements)
        midpoint = float(median(ordered))
        self.rows.append(
            ScenarioRow(
                scenario=scenario.name, mode=scenario.mode, events=scenario.events,
                median_seconds=midpoint, min_seconds=ordered[0], max_seconds=ordered[-1],
                events_per_second=scenario.events / midpoint,
            ),
        )
        self.results.extend(measurements)
        self.write_report()
        run_seconds = " ".join(f"{measurement.seconds:.2f}" for measurement in measurements)
        print(
            f"{scenario.name}: median {midpoint:.2f}s range [{ordered[0]:.2f}s, "
            f"{ordered[-1]:.2f}s] throughput {scenario.events / midpoint:.0f} events/s "
            f"runs [{run_seconds}]",
        )

    def write_report(self) -> None:
        payload = {
            "started": self.started_at.isoformat(),
            "measurements": [asdict(measurement) for measurement in self.results],
            "rows": [asdict(report_row) for report_row in self.rows],
        }
        self.results_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def print_report(self) -> None:
        print(f"results: {self.results_path}")
        for report_row in self.rows:
            print(
                f"{report_row.scenario}: median {report_row.median_seconds:.2f}s range "
                f"[{report_row.min_seconds:.2f}s, {report_row.max_seconds:.2f}s] "
                f"throughput {report_row.events_per_second:.0f} events/s",
            )
        clean = next((r for r in self.rows if r.scenario == RECOVERY_CLEAN.name), None)
        resumed = next((r for r in self.rows if r.scenario == RECOVERY_RESUME.name), None)
        if clean is not None and resumed is not None:
            added = resumed.median_seconds - clean.median_seconds
            print(f"recovery: added wall time vs a clean run, medians: {added:+.2f}s")
