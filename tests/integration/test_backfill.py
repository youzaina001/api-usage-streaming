"""Dated archive backfill, validation, partial publication, and reconstruction."""
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, Message, Producer

from tests.archive_builder import (
    ARCHIVE,
    EVENT_C,
    EVENT_D,
    MANIFEST,
    MIB_16,
    bulk_rows,
    event,
    manifest,
    row,
    write_archive,
)
from usage_stream import cli, source

from .conftest import Demo, fresh_demo
from .helpers import (
    REFERENCE_DAYS,
    checked,
    outcomes,
    read_records,
    snapshot,
    state,
    watermarks,
)
from .test_ingestion import fixture_deliveries, publish

C = event(EVENT_C, "2026-11-22T00:30:00+01:00", 5, 1)
D = event(EVENT_D, "2026-11-21T18:00:00Z", 7, 3)
VALID = row(C) + b"\n" + row(D) + b"\n"
OLDER_DAY = ("2026-11-21", "project-a", "model-a", 2, 12, 4)


def backfill(demo: Demo, archive: Path, manifest_path: Path) -> subprocess.CompletedProcess[str]:
    return demo.cli("backfill", "--file", str(archive), "--manifest", str(manifest_path))


def call_backfill(
    demo: Demo, monkeypatch: pytest.MonkeyPatch, archive: Path, manifest_path: Path,
) -> int:
    for name, value in demo.cli_env().items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(sys, "argv", [
        "usage-stream", "backfill", "--file", str(archive), "--manifest", str(manifest_path),
    ])
    return cli.main()


def test_reconstruction_replays_archive_into_fresh_topic_and_database(demo: Demo) -> None:
    checked(demo.cli("setup"))
    checked(demo.cli("publish-fixture"))
    checked(demo.cli("consume", "--until-high-watermark"))
    original = snapshot(demo)
    assert original == (REFERENCE_DAYS, (6, 3, 1, 1, 1))

    rebuilt = fresh_demo()
    overrides = (
        "--broker", rebuilt.broker, "--topic", rebuilt.topic, "--pipeline", "usage",
        "--installer-dsn", rebuilt.dsn("installer", "usage-installer-local"),
        "--consumer-dsn", rebuilt.dsn("usage_consumer", "usage-consumer-local"),
    )

    def run(*args: str) -> str:
        # The environment still names the original topic and database; the flags must win.
        return checked(subprocess.run(
            [str(Path(sys.executable).with_name("usage-stream")), *overrides, *args],
            env=os.environ | demo.cli_env(), check=False, capture_output=True, text=True, timeout=45,
        ))

    assert run("setup") == "Next offset 0"
    assert run("backfill", "--file", str(ARCHIVE), "--manifest", str(MANIFEST)) == (
        "Acknowledged 2 deliveries"
    )
    assert run("consume", "--until-high-watermark") == "Next offset 2"
    assert snapshot(rebuilt) == ([OLDER_DAY], (2, 2, 0, 0, 0))
    assert snapshot(demo) == original
    assert watermarks(demo) == (0, 6)


@pytest.mark.parametrize(("data", "fields", "reason"), [
    pytest.param(VALID, manifest(VALID, 2) | {"sha256": "0" * 64}, "checksum", id="checksum"),
    pytest.param(VALID, manifest(VALID, 3), "row count", id="row-count"),
    pytest.param(
        row(C) + b"\n" + row(D | {"event_time": "2026-11-22T00:00:00Z"}) + b"\n",
        None, "outside 2026-11-21", id="wrong-utc-day",
    ),
    pytest.param(VALID + b"\n", None, "blank", id="blank-last-row"),
    pytest.param(row(C) + b"\n" + b'{"event_id"' + b"\n", None, "row 2 is invalid", id="malformed-last-row"),
    pytest.param(row(C) + b"\n" + row(D, 16_385) + b"\n", None, "16,384", id="oversized-line"),
])
def test_invalid_archive_leaves_high_watermark_unchanged(
    demo: Demo, tmp_path: Path, data: bytes, fields: dict[str, object] | None, reason: str,
) -> None:
    checked(demo.cli("setup"))
    assert publish(demo, fixture_deliveries()[:1]) == [0]
    before = watermarks(demo)
    archive, manifest_path = write_archive(tmp_path, data, fields or manifest(data, 2))
    assert reason in checked(backfill(demo, archive, manifest_path), 2)
    assert watermarks(demo) == before == (0, 1)


@pytest.mark.parametrize(("data", "rows", "accepted"), [
    pytest.param(lambda: bulk_rows(1024, 16_383), 1024, True, id="exactly-16-mib"),
    pytest.param(lambda: bulk_rows(1023, 16_383) + bulk_rows(1, 16_384), 1024, False, id="16-mib-plus-1"),
    pytest.param(lambda: bulk_rows(50_000), 50_000, True, id="exactly-50000-rows"),
    pytest.param(lambda: bulk_rows(50_001), 50_000, False, id="50001-rows-below-16-mib"),
])
def test_archive_limits_at_cli_boundary(
    demo: Demo, tmp_path: Path, data: Callable[[], bytes], rows: int, accepted: bool,
) -> None:
    checked(demo.cli("setup"))
    content = data()
    assert len(content) <= MIB_16 + 1
    archive, manifest_path = write_archive(tmp_path, content, manifest(content, rows))
    result = backfill(demo, archive, manifest_path)
    if accepted:
        assert checked(result) == f"Acknowledged {rows} deliveries"
        assert watermarks(demo) == (0, rows)
    else:
        checked(result, 2)
        assert watermarks(demo) == (0, 0)


def test_backfill_publishes_only_validated_bytes(
    demo: Demo, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checked(demo.cli("setup"))
    archive, manifest_path = write_archive(tmp_path, VALID, manifest(VALID, 2))
    validate = source.read_archive

    def substitute_after_validation(file: Path, manifest_file: Path) -> list[tuple[bytes, bytes]]:
        validated = validate(file, manifest_file)
        file.write_bytes(row(D | {"input_tokens": 99}) + b"\n" + row(C) + b"\n")
        return validated

    monkeypatch.setattr(cli, "read_archive", substitute_after_validation)
    assert call_backfill(demo, monkeypatch, archive, manifest_path) == 0, capsys.readouterr()
    assert [(record.key(), record.value()) for record in read_records(demo, 0, 2)] == [
        (EVENT_C.encode(), row(C)), (EVENT_D.encode(), row(D)),
    ]
    assert watermarks(demo) == (0, 2)


def test_in_file_conflict_uses_first_valid_row(demo: Demo, tmp_path: Path) -> None:
    checked(demo.cli("setup"))
    data = row(D) + b"\n" + row(D | {"input_tokens": 8}) + b"\n" + row(D) + b"\n"
    archive, manifest_path = write_archive(tmp_path, data, manifest(data, 3))
    assert checked(backfill(demo, archive, manifest_path)) == "Acknowledged 3 deliveries"
    assert checked(demo.cli("consume", "--until-high-watermark")) == "Next offset 3"
    assert outcomes(demo) == [(0, "accepted"), (1, "conflict"), (2, "duplicate")]
    assert snapshot(demo) == ([("2026-11-21", "project-a", "model-a", 1, 7, 3)], (3, 1, 1, 1, 0))


class FaultyProducer(Producer):
    """Real producer with one injected fault on the second requested row."""

    mode = ""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self.requested = 0
        self.failed: list[Callable[[KafkaError | None, Message | None], None]] = []
        self.hidden = 0

    def produce(self, *args: Any, **kwargs: Any) -> None:
        self.requested += 1
        if self.requested == 2 and self.mode == "failed-delivery":
            # Injected: this row never reaches the broker and fails at flush.
            self.failed.append(kwargs["on_delivery"])
            return
        if self.requested == 2 and self.mode == "lost-acknowledgement":
            # Injected: the broker receives this row but its acknowledgement is withheld.
            kwargs.pop("on_delivery")
            self.hidden += 1
        super().produce(*args, **kwargs)

    def flush(self, *args: Any) -> int:
        pending = int(super().flush(*args))
        for callback in self.failed:
            callback(KafkaError(KafkaError._MSG_TIMED_OUT), None)
        return pending + self.hidden


@pytest.mark.parametrize(("mode", "broker_high", "retry_keys"), [
    pytest.param("failed-delivery", 1, [EVENT_C, EVENT_C, EVENT_D], id="failed-delivery"),
    pytest.param("lost-acknowledgement", 2, [EVENT_C, EVENT_D, EVENT_C, EVENT_D], id="lost-ack"),
])
def test_partial_publication_is_retryable(
    demo: Demo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    mode: str, broker_high: int, retry_keys: list[str],
) -> None:
    checked(demo.cli("setup"))
    monkeypatch.setattr(FaultyProducer, "mode", mode)
    monkeypatch.setattr(source, "Producer", FaultyProducer)
    assert call_backfill(demo, monkeypatch, ARCHIVE, MANIFEST) == 5
    assert "Publication uncertain: 1 of 2 deliveries confirmed" in capsys.readouterr().err
    assert watermarks(demo) == (0, broker_high)

    monkeypatch.setattr(source, "Producer", Producer)
    assert checked(backfill(demo, ARCHIVE, MANIFEST)) == "Acknowledged 2 deliveries"
    total = len(retry_keys)
    assert [record.key() for record in read_records(demo, 0, total)] == [
        key.encode() for key in retry_keys
    ]
    assert checked(demo.cli("consume", "--until-high-watermark")) == f"Next offset {total}"
    assert snapshot(demo) == ([OLDER_DAY], (total, 2, total - 2, 0, 0))
    assert state(demo) == (total, total, 2)
