"""Shared broker, database, and child-process helpers for recovery and backfill tests."""
import os
import subprocess
import sys
from base64 import urlsafe_b64decode
from collections.abc import Callable, Sequence
from pathlib import Path
from time import monotonic, sleep
from typing import Any
from uuid import UUID, uuid4

import psycopg
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicCollection,
    TopicPartition,
)
from psycopg.rows import TupleRow

from .conftest import ROOT, Demo

Row = tuple[str, str, str, int, int, int]
# Hand-written expected totals, independent of the decoder and consumer.
REFERENCE_DAYS: list[Row] = [
    ("2026-11-21", "project-a", "model-a", 1, 5, 1),
    ("2026-11-22", "project-a", "model-a", 2, 30, 6),
]
BACKFILLED_DAYS: list[Row] = [
    ("2026-11-21", "project-a", "model-a", 2, 12, 4),
    ("2026-11-22", "project-a", "model-a", 2, 30, 6),
]
CHILD = ROOT / "tests" / "integration" / "barrier_child.py"


def consumer_conn(demo: Demo) -> psycopg.Connection[TupleRow]:
    return psycopg.connect(demo.dsn("usage_consumer", "usage-consumer-local"))


def state(demo: Demo) -> tuple[int, int, int]:
    """Return the saved cursor and the delivery and fact row counts."""
    with consumer_conn(demo) as conn:
        row = conn.execute("""
            SELECT (SELECT next_offset FROM ops.consumer_checkpoint WHERE pipeline_id = 'usage'),
                   (SELECT count(*) FROM ops.delivery),
                   (SELECT count(*) FROM warehouse.usage_event)
        """).fetchone()
    assert row is not None
    return (int(row[0]), int(row[1]), int(row[2]))


def outcomes(demo: Demo) -> list[tuple[int, str]]:
    with consumer_conn(demo) as conn:
        rows = conn.execute('SELECT "offset", outcome FROM ops.delivery ORDER BY "offset"').fetchall()
    return [(int(offset), str(outcome)) for offset, outcome in rows]


def snapshot(demo: Demo) -> tuple[list[Row], tuple[int, ...]]:
    """Read both reporting views in one read-only snapshot as the reporting login."""
    with demo.report() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        days = conn.execute("""
            SELECT usage_day::text, project_id, model_id, request_count, input_tokens, output_tokens
            FROM reporting.daily_usage ORDER BY usage_day, project_id, model_id
        """).fetchall()
        reconciliation = conn.execute("SELECT * FROM reporting.delivery_reconciliation").fetchone()
    assert reconciliation is not None
    return [tuple(row) for row in days], tuple(reconciliation)


def checked(result: subprocess.CompletedProcess[str], code: int = 0) -> str:
    assert result.returncode == code, (result.returncode, result.stdout, result.stderr)
    return result.stdout.strip() if code == 0 else result.stderr


def wait_for(predicate: Callable[[], bool], what: str, timeout: float = 30) -> None:
    deadline = monotonic() + timeout
    while not predicate():
        if monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for {what}")
        sleep(0.05)


class Child:
    """A usage-stream child process paused at private seams through a file barrier."""

    def __init__(
        self, demo: Demo, children: list[subprocess.Popen[str]], barrier: Path,
        stages: Sequence[str], *args: str,
    ) -> None:
        barrier.mkdir(exist_ok=True)
        self.barrier = barrier
        env = os.environ | demo.cli_env() | {
            "USAGE_TEST_BARRIER_DIR": str(barrier),
            "USAGE_TEST_BARRIER_STAGES": ",".join(stages),
        }
        self.process = subprocess.Popen(
            [sys.executable, str(CHILD), *args], env=env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        children.append(self.process)

    def reached(self, stage: str, timeout: float = 30) -> int:
        marker = self.barrier / f"{stage}.reached"

        def arrived() -> bool:
            if self.process.poll() is not None:
                _, stderr = self.process.communicate()
                raise AssertionError(f"child exited {self.process.returncode} before {stage}: {stderr}")
            return marker.exists()

        wait_for(arrived, f"child at {stage}", timeout)
        return int(marker.read_text(encoding="ascii"))

    def release(self, stage: str) -> None:
        partial = self.barrier / f"{stage}.release.tmp"
        partial.write_text("continue", encoding="ascii")
        partial.rename(self.barrier / f"{stage}.release")

    def finish(self, timeout: float = 45) -> tuple[int, str, str]:
        stdout, stderr = self.process.communicate(timeout=timeout)
        return self.process.returncode, stdout, stderr


def start_cli(demo: Demo, children: list[subprocess.Popen[str]], *args: str) -> subprocess.Popen[str]:
    process = subprocess.Popen(
        [str(Path(sys.executable).with_name("usage-stream")), *args],
        env=os.environ | demo.cli_env(), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    children.append(process)
    return process


def lock_waiters(demo: Demo) -> int:
    """Count sessions in the test database waiting on the checkpoint row lock."""
    with psycopg.connect(demo.dsn("postgres", "usage-bootstrap-local"), dbname="postgres") as conn:
        row = conn.execute("""
            SELECT count(*) FROM pg_stat_activity
            WHERE datname = %s AND wait_event_type = 'Lock' AND query ILIKE '%%FOR UPDATE%%'
        """, (demo.db,)).fetchone()
    assert row is not None
    return int(row[0])


def watermarks(demo: Demo) -> tuple[int, int]:
    consumer = Consumer({"bootstrap.servers": demo.broker, "group.id": f"probe-{uuid4().hex}"})
    try:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(demo.topic, 0), timeout=10, cached=False,
        )
        return int(low), int(high)
    finally:
        consumer.close()


def topic_id(demo: Demo, timeout: float = 10) -> UUID:
    # Keep the client referenced until result() returns, or librdkafka destroys it mid-request.
    admin = demo.admin()
    description = admin.describe_topics(
        TopicCollection([demo.topic]), request_timeout=timeout,
    )[demo.topic].result(timeout=timeout + 1)
    return UUID(bytes=urlsafe_b64decode(f"{description.topic_id}=="))


def saved_topic_id(demo: Demo) -> UUID:
    with consumer_conn(demo) as conn:
        row = conn.execute("SELECT topic_id FROM ops.consumer_checkpoint").fetchone()
    assert row is not None and isinstance(row[0], UUID)
    return row[0]


def replace_topic(demo: Demo, timeout: float = 15) -> None:
    """Delete the topic and create a new generation with the same name and config."""
    admin = demo.admin()
    admin.delete_topics(
        [demo.topic], operation_timeout=timeout, request_timeout=timeout,
    )[demo.topic].result(timeout=timeout + 1)
    deadline = monotonic() + 30
    while True:
        try:
            demo.new_topic()
            return
        except KafkaException as exc:
            error = exc.args[0]
            retry = isinstance(error, KafkaError) and error.code() in {
                KafkaError.TOPIC_ALREADY_EXISTS, KafkaError.UNKNOWN_TOPIC_OR_PART,
            }
            if not retry or monotonic() > deadline:
                raise
            sleep(0.2)


def delete_before(demo: Demo, offset: int, timeout: float = 10) -> int:
    partition = TopicPartition(demo.topic, 0, offset)
    admin = demo.admin()
    result = admin.delete_records(
        [partition], request_timeout=timeout, operation_timeout=timeout,
    )[partition].result(timeout=timeout + 1)
    return int(result.low_watermark)


def read_records(demo: Demo, start: int, count: int) -> list[Message]:
    consumer = Consumer({
        "bootstrap.servers": demo.broker, "group.id": f"reader-{uuid4().hex}",
        "enable.auto.commit": False, "auto.offset.reset": "error",
        "isolation.level": "read_committed",
    })
    records: list[Message] = []
    try:
        consumer.assign([TopicPartition(demo.topic, 0, start)])
        deadline = monotonic() + 15
        while len(records) < count and monotonic() < deadline:
            for message in consumer.consume(count - len(records), 1.0):
                assert message.error() is None, message.error()
                records.append(message)
    finally:
        consumer.close()
    assert len(records) == count
    return records


class FaultyProducer(Producer):
    """Real producer with injected faults on the first requested rows."""

    mode = ""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self.requested = 0
        self.failed: list[Callable[[KafkaError | None, Message | None], None]] = []
        self.hidden = 0

    def produce(self, *args: Any, **kwargs: Any) -> None:
        self.requested += 1
        if self.mode.startswith("fail-first-"):
            # Injected: the first N rows never reach the broker and fail at flush.
            if self.requested <= int(self.mode.removeprefix("fail-first-")):
                self.failed.append(kwargs["on_delivery"])
                return
        elif self.requested == 2 and self.mode == "failed-delivery":
            # Injected: this row never reaches the broker and fails at flush.
            self.failed.append(kwargs["on_delivery"])
            return
        elif self.requested == 2 and self.mode == "lost-acknowledgement":
            # Injected: the broker receives this row but its acknowledgement is withheld.
            kwargs.pop("on_delivery")
            self.hidden += 1
        super().produce(*args, **kwargs)

    def flush(self, *args: Any) -> int:
        pending = int(super().flush(*args))
        for callback in self.failed:
            callback(KafkaError(KafkaError._MSG_TIMED_OUT), None)
        return pending + self.hidden
