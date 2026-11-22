"""Crash, concurrency, and source identity recovery against real Kafka and PostgreSQL."""
import signal
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import psycopg
import pytest
from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME

from tests.archive_builder import ARCHIVE, EVENT_C, EVENT_D, MANIFEST
from usage_stream import _seams, cli
from usage_stream.storage import Delivery, StaleCheckpoint, Storage

from .conftest import Demo
from .helpers import (
    BACKFILLED_DAYS,
    REFERENCE_DAYS,
    Child,
    checked,
    consumer_conn,
    delete_before,
    lock_waiters,
    outcomes,
    read_records,
    replace_topic,
    saved_topic_id,
    snapshot,
    start_cli,
    state,
    topic_id,
    wait_for,
    watermarks,
)
from .test_ingestion import fixture_deliveries, publish

FIXTURE_OUTCOMES = ["accepted", "accepted", "duplicate", "conflict", "invalid", "accepted"]
CONSUME = ("consume", "--until-high-watermark", "--batch-size", "2", "--timeout-seconds", "60")


def call_cli(demo: Demo, monkeypatch: pytest.MonkeyPatch, *args: str) -> int:
    for name, value in demo.cli_env().items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(sys, "argv", ["usage-stream", *args])
    return cli.main()


def published_fixture(demo: Demo) -> None:
    checked(demo.cli("setup"))
    assert checked(demo.cli("publish-fixture")) == "Acknowledged 6 deliveries"


def assert_reference(demo: Demo) -> None:
    assert state(demo) == (6, 6, 3)
    assert snapshot(demo) == (REFERENCE_DAYS, (6, 3, 1, 1, 1))
    assert outcomes(demo) == list(enumerate(FIXTURE_OUTCOMES))


@pytest.mark.parametrize(("seam", "paused"), [
    pytest.param("before_commit", (0, 0, 0), id="before-commit"),
    pytest.param("after_commit", (2, 2, 2), id="after-commit"),
])
def test_seeded_run_converges(
    demo: Demo, children: list[subprocess.Popen[str]], tmp_path: Path,
    seam: str, paused: tuple[int, int, int],
) -> None:
    published_fixture(demo)
    child = Child(demo, children, tmp_path / "barrier", [seam], *CONSUME)
    assert child.reached(seam) == 2
    assert state(demo) == paused
    if paused[1]:
        assert outcomes(demo) == [(0, "accepted"), (1, "accepted")]
    child.process.send_signal(signal.SIGKILL)
    assert child.finish()[0] == -signal.SIGKILL
    assert state(demo) == paused

    assert checked(demo.cli(*CONSUME)) == "Next offset 6"
    assert_reference(demo)

    started = datetime.now(UTC)
    assert checked(demo.cli("backfill", "--file", str(ARCHIVE), "--manifest", str(MANIFEST))) == (
        "Acknowledged 2 deliveries"
    )
    assert checked(demo.cli(*CONSUME)) == "Next offset 8"
    days, reconciliation = snapshot(demo)
    assert days[0] == BACKFILLED_DAYS[0]
    assert reconciliation == (8, 4, 2, 1, 1)

    assert checked(demo.cli("backfill", "--file", str(ARCHIVE), "--manifest", str(MANIFEST))) == (
        "Acknowledged 2 deliveries"
    )
    assert checked(demo.cli(*CONSUME)) == "Next offset 10"
    assert snapshot(demo) == (BACKFILLED_DAYS, (10, 4, 4, 1, 1))
    assert state(demo) == (10, 10, 4)

    records = read_records(demo, 6, 4)
    assert [record.key() for record in records] == [
        EVENT_C.encode(), EVENT_D.encode(), EVENT_C.encode(), EVENT_D.encode(),
    ]
    for record in records:
        kind, millis = record.timestamp()
        assert kind == TIMESTAMP_LOG_APPEND_TIME
        appended = datetime.fromtimestamp(millis / 1000, UTC)
        assert -5 < (appended - started).total_seconds() < 120
    with consumer_conn(demo) as conn:
        facts = conn.execute("""
            SELECT event_id::text, (event_time AT TIME ZONE 'UTC')::date::text, "offset"
            FROM warehouse.usage_event WHERE event_id = ANY(%s) ORDER BY "offset"
        """, ([UUID(EVENT_C), UUID(EVENT_D)],)).fetchall()
    assert facts == [(EVENT_C, "2026-11-21", 5), (EVENT_D, "2026-11-21", 7)]


def test_stale_batch_cannot_commit(
    demo: Demo, children: list[subprocess.Popen[str]], tmp_path: Path,
) -> None:
    published_fixture(demo)
    first = Child(demo, children, tmp_path / "barrier", ["before_commit", "after_commit"], *CONSUME)
    assert first.reached("before_commit") == 2
    assert state(demo) == (0, 0, 0)

    second = start_cli(demo, children, *CONSUME)
    wait_for(lambda: lock_waiters(demo) == 1, "second worker to wait on the checkpoint lock", 15)
    assert second.poll() is None
    first.release("before_commit")
    assert first.reached("after_commit") == 2

    _, stderr = second.communicate(timeout=20)
    assert second.returncode == 4, stderr
    assert "Stale checkpoint" in stderr
    assert state(demo) == (2, 2, 2)
    assert outcomes(demo) == [(0, "accepted"), (1, "accepted")]

    first.release("after_commit")
    code, stdout, stderr = first.finish()
    assert code == 0, stderr
    assert stdout.strip() == "Next offset 6"
    assert_reference(demo)


def test_stale_batch_cannot_commit_ahead_of_saved_cursor(demo: Demo) -> None:
    checked(demo.cli("setup"))
    with psycopg.connect(demo.dsn("usage_consumer", "usage-consumer-local"), autocommit=True) as conn:
        storage = Storage(conn)
        checkpoint = storage.read_checkpoint("usage")
        assert checkpoint is not None
        batch = [
            Delivery(checkpoint.topic_id, 0, offset, key, payload)
            for offset, (key, payload) in enumerate(fixture_deliveries()[2:4], start=2)
        ]
        with pytest.raises(StaleCheckpoint):
            storage.apply_batch("usage", checkpoint.topic_id, 0, 2, batch)
        assert storage.read_checkpoint("usage") == checkpoint
    assert state(demo) == (0, 0, 0)


def test_failed_insert_rolls_back_whole_batch(demo: Demo) -> None:
    published_fixture(demo)
    with psycopg.connect(demo.dsn("installer", "usage-installer-local")) as conn:
        conn.execute("""
            CREATE FUNCTION ops.fail_second_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'injected insert failure at offset %', NEW."offset";
            END;
            $$
        """)
        conn.execute("""
            CREATE TRIGGER fail_second_delivery BEFORE INSERT ON ops.delivery
            FOR EACH ROW WHEN (NEW."offset" = 1) EXECUTE FUNCTION ops.fail_second_delivery()
        """)
    stderr = checked(demo.cli(*CONSUME), 5)
    assert "RaiseException" in stderr
    assert state(demo) == (0, 0, 0)

    with psycopg.connect(demo.dsn("installer", "usage-installer-local")) as conn:
        conn.execute("DROP TRIGGER fail_second_delivery ON ops.delivery")
    assert checked(demo.cli(*CONSUME)) == "Next offset 6"
    assert_reference(demo)


def test_lost_commit_acknowledgement_resumes_after_commit(
    demo: Demo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    published_fixture(demo)

    def lose_first_acknowledgement(stage: str, _offset: int) -> None:
        if stage == "after_commit":
            _seams.install(None)
            raise psycopg.OperationalError("injected loss of the commit acknowledgement")

    _seams.install(lose_first_acknowledgement)
    try:
        assert call_cli(demo, monkeypatch, *CONSUME) == 5, capsys.readouterr()
    finally:
        _seams.install(None)
    assert state(demo) == (2, 2, 2)

    batches: list[tuple[int, list[int]]] = []
    apply_batch = Storage.apply_batch

    def recorded(
        storage: Storage, pipeline_id: str, expected_topic_id: UUID, partition: int,
        next_offset: int, deliveries: Sequence[Delivery],
    ) -> int:
        batches.append((next_offset, [delivery.offset for delivery in deliveries]))
        return apply_batch(storage, pipeline_id, expected_topic_id, partition, next_offset, deliveries)

    monkeypatch.setattr(Storage, "apply_batch", recorded)
    assert call_cli(demo, monkeypatch, *CONSUME) == 0, capsys.readouterr()
    assert batches[0][0] == 2
    assert [offset for _, offsets in batches for offset in offsets] == [2, 3, 4, 5]
    assert_reference(demo)


def test_topic_identity_and_offset_guard_rejects_recreated_topic(demo: Demo) -> None:
    checked(demo.cli("setup"))
    assert publish(demo, fixture_deliveries()[:2]) == [0, 1]
    assert checked(demo.cli(*CONSUME)) == "Next offset 2"
    original = saved_topic_id(demo)
    replace_topic(demo)
    assert publish(demo, fixture_deliveries()[:3]) == [0, 1, 2]
    assert topic_id(demo) != original
    assert watermarks(demo) == (0, 3)
    assert "topic identity changed" in checked(demo.cli(*CONSUME), 3)
    assert state(demo) == (2, 2, 2)


def test_topic_identity_and_offset_guard_detects_replacement_after_fetch(
    demo: Demo, children: list[subprocess.Popen[str]], tmp_path: Path,
) -> None:
    published_fixture(demo)
    original = saved_topic_id(demo)
    child = Child(demo, children, tmp_path / "barrier", ["fetched"], *CONSUME)
    assert child.reached("fetched") == 1
    replace_topic(demo)
    assert publish(demo, fixture_deliveries()) == list(range(6))
    assert topic_id(demo) != original
    assert watermarks(demo) == (0, 6)
    child.release("fetched")
    code, _, stderr = child.finish()
    assert code == 3, stderr
    # The post-fetch guard must name the identity change, not a bounds or lookup failure.
    assert "Source guard: topic identity changed" in stderr
    assert state(demo) == (0, 0, 0)


def test_topic_identity_and_offset_guard_rejects_expired_cursor_at_startup(demo: Demo) -> None:
    checked(demo.cli("setup"))
    assert publish(demo, fixture_deliveries()[:2]) == [0, 1]
    assert checked(demo.cli(*CONSUME)) == "Next offset 2"
    assert publish(demo, fixture_deliveries()[2:4]) == [2, 3]
    assert delete_before(demo, 3) == 3
    assert watermarks(demo) == (3, 4)
    assert "outside [3, 4]" in checked(demo.cli(*CONSUME), 3)
    assert state(demo) == (2, 2, 2)


def test_topic_identity_and_offset_guard_rejects_expiry_during_run(
    demo: Demo, children: list[subprocess.Popen[str]], tmp_path: Path,
) -> None:
    published_fixture(demo)
    child = Child(demo, children, tmp_path / "barrier", ["fetched"], *CONSUME)
    assert child.reached("fetched") == 1
    assert delete_before(demo, 2) == 2
    assert watermarks(demo) == (2, 6)
    child.release("fetched")
    code, _, stderr = child.finish()
    assert code == 3, stderr
    assert "outside [2, 6]" in stderr
    assert state(demo) == (0, 0, 0)


def test_topic_identity_and_offset_guard_rejects_cursor_above_high_watermark(demo: Demo) -> None:
    checked(demo.cli("setup"))
    assert publish(demo, fixture_deliveries()[:2]) == [0, 1]
    with psycopg.connect(demo.dsn("postgres", "usage-bootstrap-local")) as conn:
        conn.execute("UPDATE ops.consumer_checkpoint SET next_offset = 5")
    assert watermarks(demo) == (0, 2)
    assert "outside [0, 2]" in checked(demo.cli(*CONSUME), 3)
    assert state(demo) == (5, 0, 0)
