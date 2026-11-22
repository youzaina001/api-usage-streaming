import json
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka import (
    KafkaError,
    Message,
    Producer,
    TopicCollection,
    TopicPartition,
)
from confluent_kafka.admin import ConfigResource, ResourceType  # type: ignore[attr-defined]

from usage_stream.storage import Delivery, SourceGuardError, Storage

from .conftest import FIXTURES, ROOT, TOPIC_CONFIG, Demo


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
