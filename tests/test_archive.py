"""Broker-free checks for archive validation and publication accounting."""
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

import pytest
from confluent_kafka import KafkaError

from tests.archive_builder import (
    ARCHIVE,
    DAY,
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
from usage_stream.source import ArchiveError, PublicationUncertain, read_archive
from usage_stream.storage import SourceGuardError

C = event(EVENT_C, "2026-11-22T00:30:00+01:00", 5, 1)
D = event(EVENT_D, "2026-11-21T18:00:00Z", 7, 3)
VALID = row(C) + b"\n" + row(D) + b"\n"


def test_dated_fixture_matches_manifest_and_keeps_file_order() -> None:
    assert ARCHIVE.read_bytes() == VALID
    assert read_archive(ARCHIVE, MANIFEST) == [
        (EVENT_C.encode(), row(C)), (EVENT_D.encode(), row(D)),
    ]


def test_terminal_lf_is_optional(tmp_path: Path) -> None:
    data = VALID[:-1]
    assert len(read_archive(*write_archive(tmp_path, data, manifest(data, 2)))) == 2


def test_in_file_conflicts_are_rejected(tmp_path: Path) -> None:
    changed = row(D | {"input_tokens": 8})
    data = row(D) + b"\n" + changed + b"\n" + row(D) + b"\n"
    with pytest.raises(ArchiveError, match=f"row 2 conflicts with row 1 for event id {EVENT_D}"):
        read_archive(*write_archive(tmp_path, data, manifest(data, 3)))


def test_in_file_repeats_with_equal_facts_are_kept_in_order(tmp_path: Path) -> None:
    respelled = row(event(EVENT_D, "2026-11-21T19:00:00+01:00", 7, 3))
    data = row(D) + b"\n" + respelled + b"\n" + row(D) + b"\n"
    assert [payload for _, payload in read_archive(
        *write_archive(tmp_path, data, manifest(data, 3)),
    )] == [row(D), respelled, row(D)]


@pytest.mark.parametrize(("data", "fields", "reason"), [
    pytest.param(VALID, manifest(VALID, 2) | {"sha256": "0" * 64}, "checksum", id="checksum"),
    pytest.param(VALID, manifest(VALID, 3), "row count", id="row-count"),
    pytest.param(
        row(C) + b"\n" + row(D | {"event_time": "2026-11-22T00:00:00Z"}) + b"\n",
        None, "outside 2026-11-21", id="wrong-utc-day",
    ),
    pytest.param(VALID + b"\n", None, "blank", id="blank-last-row"),
    pytest.param(row(C) + b"\n\n" + row(D), None, "blank", id="blank-middle-row"),
    pytest.param(b"\n", None, "blank", id="only-lf"),
    pytest.param(b"", None, "blank", id="empty"),
    pytest.param(row(C) + b"\n" + b'{"event_id"' + b"\n", None, "row 2 is invalid", id="malformed-last-row"),
    pytest.param(
        row(C) + b"\n" + row(D | {"input_tokens": -1}), None, "row 2 is invalid", id="invalid-event",
    ),
    pytest.param(row(C) + b"\n" + row(D, 16_385) + b"\n", None, "larger than 16,384", id="oversized-line"),
    pytest.param(row(C) + b"\r\n" + row(D) + b"\r\n", None, "LF", id="crlf"),
    pytest.param(row(C) + b"\n" + b"\xff" + row(D), None, "row 2 is invalid", id="invalid-utf8"),
    pytest.param(
        row(C) + b"\n" + row(C | {"event_id": "\ud800"}) + b"\n",
        None, "row 2 is invalid", id="lone-surrogate-event-id",
    ),
])
def test_invalid_archives_are_rejected(
    tmp_path: Path, data: bytes, fields: dict[str, object] | None, reason: str,
) -> None:
    if fields is None:
        rows = data.rstrip(b"\n").split(b"\n") if data.strip(b"\n") else []
        fields = manifest(data, max(1, len([part for part in rows if part])))
    with pytest.raises(ArchiveError, match=reason):
        read_archive(*write_archive(tmp_path, data, fields))


@pytest.mark.parametrize(("fields", "reason"), [
    ({"day": DAY, "sha256": "0" * 64}, "exactly"),
    ({"day": DAY, "sha256": "0" * 64, "row_count": 2, "extra": 1}, "exactly"),
    ({"day": "20261121", "sha256": "0" * 64, "row_count": 2}, "day"),
    ({"day": "2026-02-30", "sha256": "0" * 64, "row_count": 2}, "day"),
    ({"day": DAY, "sha256": "A" * 64, "row_count": 2}, "sha256"),
    ({"day": DAY, "sha256": "0" * 63, "row_count": 2}, "sha256"),
    ({"day": DAY, "sha256": "0" * 64, "row_count": True}, "row_count"),
    ({"day": DAY, "sha256": "0" * 64, "row_count": 0}, "row_count"),
    ({"day": DAY, "sha256": "0" * 64, "row_count": 50_001}, "row_count"),
    ({"day": DAY, "sha256": "0" * 64, "row_count": 2.0}, "row_count"),
])
def test_invalid_manifests_are_rejected(
    tmp_path: Path, fields: dict[str, object], reason: str,
) -> None:
    archive, manifest_path = write_archive(tmp_path, VALID, fields)
    with pytest.raises(ArchiveError, match=reason):
        read_archive(archive, manifest_path)


@pytest.mark.parametrize("text", [
    "[]", "not json", '{"day":"2026-11-21","day":"2026-11-21","sha256":"x","row_count":1}', "﻿{}",
])
def test_manifest_must_be_one_json_object(tmp_path: Path, text: str) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(text, encoding="utf-8")
    with pytest.raises(ArchiveError, match="manifest"):
        read_archive(ARCHIVE, manifest_path)


def test_size_limit_is_exactly_16_mib(tmp_path: Path) -> None:
    exact = bulk_rows(1024, 16_383)
    assert len(exact) == MIB_16
    assert len(read_archive(*write_archive(tmp_path, exact, manifest(exact, 1024)))) == 1024
    over = bulk_rows(1023, 16_383) + bulk_rows(1, 16_384)
    assert len(over) == MIB_16 + 1
    with pytest.raises(ArchiveError, match="larger than 16 MiB"):
        read_archive(*write_archive(tmp_path, over, manifest(over, 1024)))


def test_row_limit_is_exactly_50000(tmp_path: Path) -> None:
    exact = bulk_rows(50_000)
    assert len(read_archive(*write_archive(tmp_path, exact, manifest(exact, 50_000)))) == 50_000
    over = bulk_rows(50_001)
    assert len(over) < MIB_16
    with pytest.raises(ArchiveError, match="more than 50,000 rows"):
        read_archive(*write_archive(tmp_path, over, manifest(over, 50_000)))


def run_backfill(
    monkeypatch: pytest.MonkeyPatch, archive: Path, manifest_path: Path,
) -> int:
    monkeypatch.setattr(cli, "_require_existing_topic", lambda *args: None)
    monkeypatch.setattr(sys, "argv", [
        "usage-stream", "--broker", "127.0.0.1:1", "--topic", "unused",
        "backfill", "--file", str(archive), "--manifest", str(manifest_path),
    ])
    return cli.main()


def test_cli_backfill_requires_an_existing_topic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    published: list[object] = []

    def capture(*args: object) -> int:
        published.append(args)
        return 0

    def refuse(*args: object) -> None:
        raise SourceGuardError("topic or offset unavailable (UNKNOWN_TOPIC_OR_PART)")

    monkeypatch.setattr(cli, "publish", capture)
    monkeypatch.setattr(cli, "_require_existing_topic", refuse)
    archive, manifest_path = write_archive(tmp_path, VALID, manifest(VALID, 2))
    monkeypatch.setattr(sys, "argv", [
        "usage-stream", "--broker", "127.0.0.1:1", "--topic", "unused",
        "backfill", "--file", str(archive), "--manifest", str(manifest_path),
    ])
    assert cli.main() == 3
    assert "Source guard: " in capsys.readouterr().err
    assert published == []


def test_cli_rejects_invalid_archive_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    published: list[object] = []

    def capture(*args: object) -> int:
        published.append(args)
        return 0

    monkeypatch.setattr(cli, "publish", capture)
    archive, manifest_path = write_archive(tmp_path, VALID + b"\n", manifest(VALID + b"\n", 2))
    assert run_backfill(monkeypatch, archive, manifest_path) == 2
    assert "Archive rejected: " in capsys.readouterr().err
    assert published == []
    assert run_backfill(monkeypatch, tmp_path / "missing.jsonl", manifest_path) == 2
    assert published == []


def test_cli_publishes_only_validated_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    archive, manifest_path = write_archive(tmp_path, VALID, manifest(VALID, 2))
    validate = source.read_archive

    def substitute_after_validation(file: Path, manifest_file: Path) -> list[tuple[bytes, bytes]]:
        validated = validate(file, manifest_file)
        file.write_bytes(row(D | {"input_tokens": 99}) + b"\n" + row(C) + b"\n")
        return validated

    published: list[tuple[bytes, bytes]] = []

    def capture(_broker: str, _topic: str, deliveries: Sequence[tuple[bytes, bytes]]) -> int:
        published.extend(deliveries)
        return len(deliveries)

    monkeypatch.setattr(cli, "read_archive", substitute_after_validation)
    monkeypatch.setattr(cli, "publish", capture)
    assert run_backfill(monkeypatch, archive, manifest_path) == 0
    assert capsys.readouterr().out.strip() == "Acknowledged 2 deliveries"
    assert published == [(EVENT_C.encode(), row(C)), (EVENT_D.encode(), row(D))]
    assert archive.read_bytes() != VALID


Callback = Callable[[KafkaError | None, object], None]


class FakeProducer:
    """Broker-free producer that reports scripted delivery outcomes from flush()."""

    script: list[str] = []
    queue_full_once = False

    def __init__(self, _config: dict[str, object]) -> None:
        self.reports: list[tuple[Callback, KafkaError | None]] = []
        self.pending = 0
        self.full_once = self.queue_full_once

    def produce(
        self, _topic: str, value: bytes, key: bytes, partition: int, on_delivery: Callback,
    ) -> None:
        assert partition == 0 and key and value
        if self.full_once:
            self.full_once = False
            raise BufferError("queue full")
        outcome = self.script[len(self.reports) + self.pending] if self.script else "ok"
        if outcome == "lost":
            self.pending += 1
        else:
            failure = KafkaError(KafkaError._MSG_TIMED_OUT) if outcome == "fail" else None
            self.reports.append((on_delivery, failure))

    def poll(self, _timeout: float = 0) -> int:
        return 0

    def flush(self, _timeout: float = 0) -> int:
        for callback, error in self.reports:
            callback(error, None)
        self.reports = []
        return self.pending


@pytest.mark.parametrize(("script", "confirmed"), [
    pytest.param(["ok", "fail"], 1, id="failed-delivery"),
    pytest.param(["ok", "lost"], 1, id="lost-acknowledgement"),
    pytest.param(["fail", "fail"], 0, id="nothing-confirmed"),
])
def test_uncertain_publication_reports_confirmed_lower_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    script: list[str], confirmed: int,
) -> None:
    monkeypatch.setattr(FakeProducer, "script", script)
    monkeypatch.setattr(source, "Producer", FakeProducer)
    with pytest.raises(PublicationUncertain) as raised:
        source.publish("127.0.0.1:1", "unused", [(b"k", b"v"), (b"k", b"v")])
    assert (raised.value.confirmed, raised.value.requested) == (confirmed, 2)
    archive, manifest_path = write_archive(tmp_path, VALID, manifest(VALID, 2))
    assert run_backfill(monkeypatch, archive, manifest_path) == 5
    error = capsys.readouterr().err
    assert f"Publication uncertain: {confirmed} of 2 deliveries confirmed" in error
    assert "rerun the whole file" in error


def test_full_producer_queue_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(FakeProducer, "queue_full_once", True)
    monkeypatch.setattr(source, "Producer", FakeProducer)
    assert source.publish("127.0.0.1:1", "unused", [(b"k", b"v")]) == 1


def test_manifest_fixture_is_canonical() -> None:
    assert json.loads(MANIFEST.read_text(encoding="utf-8")) == manifest(VALID, 2)
