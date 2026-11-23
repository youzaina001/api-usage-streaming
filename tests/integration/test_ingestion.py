import json
import sys
from collections.abc import Sequence
from concurrent.futures import Future
from dataclasses import replace
from time import monotonic
from uuid import UUID, uuid4

import psycopg
import pytest
from confluent_kafka import (
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicCollection,
    TopicPartition,
)
from confluent_kafka.admin import (  # type: ignore[attr-defined]
    AdminClient,
    ConfigResource,
    ResourceType,
)
from psycopg import sql

from usage_stream import cli, consumer
from usage_stream.storage import Delivery, SourceGuardError, Storage

from .conftest import FIXTURES, ROOT, TOPIC_CONFIG, Demo
from .helpers import checked


def call_cli(demo: Demo, monkeypatch: pytest.MonkeyPatch, *args: str) -> int:
    for name, value in demo.cli_env().items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(sys, "argv", ["usage-stream", *args])
    return cli.main()


def fixture_deliveries() -> list[tuple[bytes, bytes]]:
    path = ROOT / "src" / "usage_stream" / "fixtures" / "deliveries.json"
    events = json.loads(path.read_text(encoding="utf-8"))
    return [
        (event["event_id"].encode(), json.dumps(event, separators=(",", ":")).encode())
        for event in events
    ]


def publish(demo: Demo, deliveries: list[tuple[bytes, bytes]]) -> list[int]:
    producer = Producer({"bootstrap.servers": demo.broker, "enable.idempotence": True})
    confirmations: list[int] = []
    errors: list[KafkaError] = []

    def confirmed(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            errors.append(error)
        else:
            offset = message.offset()
            assert offset is not None
            confirmations.append(offset)

    for key, payload in deliveries:
        producer.produce(demo.topic, key=key, value=payload, partition=0, on_delivery=confirmed)
    assert producer.flush(10) == 0
    assert not errors
    assert len(confirmations) == len(deliveries)
    return confirmations


def assert_fixture_totals(demo: Demo) -> None:
    expected = json.loads((FIXTURES / "expected_usage.json").read_text(encoding="utf-8"))
    with demo.report() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        rows = conn.execute("""
            SELECT usage_day::text, project_id, model_id, request_count, input_tokens, output_tokens
            FROM reporting.daily_usage ORDER BY usage_day
        """).fetchall()
        assert [list(row) for row in rows] == expected["daily_usage"]
        result = conn.execute("SELECT * FROM reporting.delivery_reconciliation").fetchone()
        assert result == tuple(expected["delivery_reconciliation"])


def test_scaffold_smoke(demo: Demo) -> None:
    demo.new_topic()
    admin = demo.admin()
    description = admin.describe_topics(
        TopicCollection([demo.topic]), request_timeout=10,
    )[demo.topic].result(timeout=11)
    assert [partition.id for partition in description.partitions] == [0]
    assert str(description.topic_id) != "AAAAAAAAAAAAAAAAAAAAAA"
    topic = ConfigResource(ResourceType.TOPIC, demo.topic)
    broker = ConfigResource(ResourceType.BROKER, "1")
    futures = admin.describe_configs([topic, broker], request_timeout=10)
    entries = futures[topic].result(timeout=11)
    assert {key: entries[key].value for key in TOPIC_CONFIG} == TOPIC_CONFIG
    assert futures[broker].result(timeout=11)["auto.create.topics.enable"].value == "false"
    with demo.report() as conn:
        assert conn.execute("SELECT * FROM reporting.daily_usage").fetchall() == []
        assert conn.execute("SELECT * FROM reporting.delivery_reconciliation").fetchone() == (
            0, 0, 0, 0, 0,
        )


def test_fixture_reconciles(demo: Demo) -> None:
    demo.new_topic()
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr
    assert publish(demo, fixture_deliveries()) == list(range(6))
    consume = demo.cli("consume", "--until-high-watermark", "--timeout-seconds", "30")
    assert consume.returncode == 0, consume.stderr
    assert_fixture_totals(demo)
    assert consume.stdout.strip() == "Next offset 6"
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr
    assert setup.stdout.strip() == "Next offset 6"


def test_fixture_cli_preserves_delivery_order_across_batches(demo: Demo) -> None:
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr
    fixture = demo.cli("publish-fixture")
    assert fixture.returncode == 0, fixture.stderr
    assert fixture.stdout.strip() == "Acknowledged 6 deliveries"
    consume = demo.cli("consume", "--until-high-watermark", "--batch-size", "2")
    assert consume.returncode == 0, consume.stderr
    assert_fixture_totals(demo)
    with psycopg.connect(demo.dsn("usage_consumer", "usage-consumer-local")) as conn:
        outcomes = conn.execute("""
            SELECT "offset", outcome, event_id::text FROM ops.delivery ORDER BY "offset"
        """).fetchall()
    assert outcomes == [
        (offset, outcome, key.decode())
        for offset, (outcome, (key, _)) in enumerate(zip(
            ("accepted", "accepted", "duplicate", "conflict", "invalid", "accepted"),
            fixture_deliveries(), strict=True,
        ))
    ]


def test_publish_fixture_into_missing_topic_exits_3(demo: Demo) -> None:
    checked(demo.cli("setup"))
    result = demo.cli("--topic", "api-usage-missing", "publish-fixture")
    assert result.returncode == 3
    assert "Source guard: " in result.stderr


def test_reporting_reader_can_query_views_only(demo: Demo) -> None:
    with demo.report() as reader:
        assert reader.execute("SELECT * FROM reporting.daily_usage").fetchall() == []
        assert reader.execute("SELECT count(*) FROM reporting.delivery_reconciliation").fetchone() == (1,)
        assert reader.execute("""
            SELECT has_table_privilege('reporting.daily_usage', 'UPDATE')
        """).fetchone() == (False,)
        for statement in (
            "SELECT * FROM ops.delivery",
            "SELECT * FROM warehouse.usage_event",
            "UPDATE warehouse.usage_event SET input_tokens = 0",
            "CREATE TABLE reporting.denied (id integer)",
            "CREATE TEMP TABLE denied (id integer)",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege), reader.transaction():
                reader.execute(statement)
    with psycopg.connect(demo.dsn("usage_consumer", "usage-consumer-local")) as writer:
        for statement in (
            "UPDATE ops.consumer_checkpoint SET topic_id = topic_id",
            "INSERT INTO ops.consumer_checkpoint DEFAULT VALUES",
            "DELETE FROM warehouse.usage_event",
            "UPDATE ops.delivery SET reason = 'changed_event'",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege), writer.transaction():
                writer.execute(statement)


def test_reporting_snapshot_stays_consistent_while_consumer_commits(demo: Demo) -> None:
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr
    with demo.report() as reader:
        reader.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        assert reader.execute("SELECT * FROM reporting.daily_usage").fetchall() == []
        assert publish(demo, fixture_deliveries()) == list(range(6))
        consume = demo.cli("consume", "--until-high-watermark")
        assert consume.returncode == 0, consume.stderr
        assert reader.execute("SELECT * FROM reporting.delivery_reconciliation").fetchone() == (
            0, 0, 0, 0, 0,
        )
    assert_fixture_totals(demo)


def test_empty_poll_leaves_checkpoint(demo: Demo) -> None:
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr
    result = demo.cli("consume", "--timeout-seconds", "1")
    assert result.returncode == 6, result.stderr
    assert "offset 0" in result.stderr
    bounded = demo.cli("consume", "--until-high-watermark")
    assert bounded.returncode == 0, bounded.stderr
    assert bounded.stdout.strip() == "Next offset 0"


def test_final_commit_crossing_deadline_times_out_with_durable_progress(
    demo: Demo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr
    assert publish(demo, fixture_deliveries()[:1]) == [0]

    apply_batch = Storage.apply_batch

    def delayed_batch(
        storage: Storage, pipeline_id: str, topic_id: UUID, partition: int,
        next_offset: int, deliveries: Sequence[Delivery],
    ) -> int:
        committed_offset = apply_batch(storage, pipeline_id, topic_id, partition, next_offset, deliveries)
        monkeypatch.setattr(consumer, "monotonic", lambda: monotonic() + 30)
        return committed_offset

    monkeypatch.setattr(Storage, "apply_batch", delayed_batch)
    result = call_cli(demo, monkeypatch, "consume", "--until-high-watermark", "--timeout-seconds", "15")
    output = capsys.readouterr()
    assert result == 6, output
    assert "offset 1" in output.err

    retry = demo.cli("consume", "--until-high-watermark")
    assert retry.returncode == 0, retry.stderr
    assert retry.stdout.strip() == "Next offset 1"
    with demo.report() as conn:
        assert conn.execute("SELECT * FROM reporting.delivery_reconciliation").fetchone() == (
            1, 1, 0, 0, 0,
        )


@pytest.mark.parametrize(("timezone", "timestamps", "first_utc"), [
    pytest.param("America/New_York", (
        "2026-11-01T01:30:00-05:00", "2026-11-01T06:30:00Z", "2026-11-01T01:30:00-04:00",
    ), "2026-11-01 06:30:00", id="dst-fold"),
    pytest.param("Pacific/Kiritimati", (
        "9999-12-31T23:59:59Z", "9999-12-31T22:59:59-01:00", "9999-12-31T22:59:59Z",
    ), "9999-12-31 23:59:59", id="local-year-10000"),
])
def test_equivalent_timestamps_are_duplicates(
    demo: Demo, timezone: str, timestamps: tuple[str, str, str], first_utc: str,
) -> None:
    with psycopg.connect(demo.dsn("postgres", "usage-bootstrap-local"), autocommit=True) as conn:
        conn.execute(sql.SQL("ALTER DATABASE {} SET TimeZone TO {}").format(
            sql.Identifier(demo.db), sql.Literal(timezone),
        ))
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr
    key, payload = fixture_deliveries()[0]
    event = json.loads(payload)
    assert publish(demo, [
        (key, json.dumps(event | {"event_time": timestamp}).encode())
        for timestamp in timestamps
    ]) == [0, 1, 2]
    result = demo.cli("consume", "--until-high-watermark")
    assert result.returncode == 0, result.stderr
    with psycopg.connect(demo.dsn("usage_consumer", "usage-consumer-local")) as conn:
        outcomes = conn.execute('SELECT outcome FROM ops.delivery ORDER BY "offset"').fetchall()
        assert outcomes == [("accepted",), ("duplicate",), ("conflict",)]
        assert conn.execute("""
            SELECT event_time AT TIME ZONE 'UTC' = %s::timestamp
            FROM warehouse.usage_event
        """, (first_utc,)).fetchone() == (True,)


def test_offsets_need_not_be_contiguous(demo: Demo) -> None:
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr
    producer = Producer({
        "bootstrap.servers": demo.broker, "transactional.id": demo.topic,
        "transaction.timeout.ms": 10_000,
    })
    producer.init_transactions(15)
    producer.begin_transaction()
    (first_key, first_value), (second_key, second_value) = fixture_deliveries()[:2]
    producer.produce(demo.topic, key=first_key, value=first_value, partition=0)
    producer.commit_transaction(15)
    [second_offset] = publish(demo, [(second_key, second_value)])
    assert second_offset > 1
    result = demo.cli("consume", "--until-high-watermark")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"Next offset {second_offset + 1}"
    with psycopg.connect(demo.dsn("usage_consumer", "usage-consumer-local")) as conn:
        rows = conn.execute('SELECT "offset", outcome FROM ops.delivery ORDER BY "offset"').fetchall()
        assert rows == [(0, "accepted"), (second_offset, "accepted")]


def test_setup_rejects_missing_origin(demo: Demo) -> None:
    demo.new_topic()
    assert publish(demo, fixture_deliveries()[:2]) == [0, 1]
    admin = demo.admin()
    partition = TopicPartition(demo.topic, 0, 1)
    result = admin.delete_records([partition], request_timeout=10)[partition].result(timeout=11)
    assert result.low_watermark == 1
    rejected = demo.cli("setup")
    assert rejected.returncode == 3, rejected.stderr
    assert "outside [1, 2]" in rejected.stderr


def test_setup_rejects_wrong_partition_count(demo: Demo) -> None:
    demo.new_topic(partitions=2)
    setup = demo.cli("setup")
    assert setup.returncode == 3, setup.stderr


@pytest.mark.parametrize(("error_code", "exit_code"), [
    (KafkaError.UNKNOWN_TOPIC_OR_PART, 3),
    (KafkaError._TIMED_OUT, 5),
])
def test_setup_classifies_config_lookup_errors(
    demo: Demo, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    error_code: int, exit_code: int,
) -> None:
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr

    def failed_configs(
        _admin: AdminClient, resources: list[ConfigResource], *, request_timeout: float,
    ) -> dict[ConfigResource, Future[object]]:
        future: Future[object] = Future()
        future.set_exception(KafkaException(KafkaError(error_code)))
        return {resources[0]: future}

    monkeypatch.setattr(AdminClient, "describe_configs", failed_configs)
    result = call_cli(demo, monkeypatch, "setup")
    assert result == exit_code, capsys.readouterr()


def test_consume_does_not_create_missing_topic(demo: Demo) -> None:
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr
    missing = replace(demo, topic=f"missing-{uuid4().hex}")
    result = missing.cli("consume", "--until-high-watermark")
    assert result.returncode == 3, result.stderr
    admin = demo.admin()
    assert missing.topic not in admin.list_topics(timeout=5).topics


def test_storage_rejects_invalid_batches_atomically(demo: Demo) -> None:
    setup = demo.cli("setup")
    assert setup.returncode == 0, setup.stderr
    with psycopg.connect(demo.dsn("usage_consumer", "usage-consumer-local"), autocommit=True) as conn:
        storage = Storage(conn)
        checkpoint = storage.read_checkpoint("usage")
        assert checkpoint is not None
        key, payload = fixture_deliveries()[0]
        valid = Delivery(checkpoint.topic_id, 0, 1, key, payload)
        for batch in (
            [replace(valid, offset=-1)],
            [valid, replace(valid, offset=0)],
            [valid, replace(valid, offset=2, partition=1)],
            [valid, replace(valid, offset=2, topic_id=uuid4())],
        ):
            with pytest.raises(SourceGuardError):
                storage.apply_batch("usage", checkpoint.topic_id, 0, 0, batch)
        assert storage.read_checkpoint("usage") == checkpoint
    with demo.report() as reader:
        assert reader.execute("SELECT * FROM reporting.daily_usage").fetchall() == []
        assert reader.execute("SELECT * FROM reporting.delivery_reconciliation").fetchone() == (
            0, 0, 0, 0, 0,
        )
