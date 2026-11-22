import json
from collections.abc import Iterable
from importlib.resources import files

from confluent_kafka import KafkaError, KafkaException, Message, Producer

TOPIC_CONFIG = {
    "cleanup.policy": "delete",
    "retention.ms": "604800000",
    "retention.bytes": "-1",
    "message.timestamp.type": "LogAppendTime",
}


def fixture_deliveries() -> list[tuple[bytes, bytes]]:
    fixture = files("usage_stream").joinpath("fixtures", "deliveries.json")
    return [
        (event["event_id"].encode(), json.dumps(event, separators=(",", ":")).encode())
        for event in json.loads(fixture.read_text(encoding="utf-8"))
    ]


def publish(broker: str, topic: str, deliveries: Iterable[tuple[bytes, bytes]]) -> int:
    producer = Producer({
        "bootstrap.servers": broker,
        "allow.auto.create.topics": False,
        "enable.idempotence": True,
        "message.timeout.ms": 10_000,
    })
    submitted = acknowledged = 0
    first_error: KafkaError | None = None

    def complete(error: KafkaError | None, _message: Message) -> None:
        nonlocal acknowledged, first_error
        if error is None:
            acknowledged += 1
        elif first_error is None:
            first_error = error

    for key, payload in deliveries:
        producer.produce(topic, value=payload, key=key, partition=0, on_delivery=complete)
        submitted += 1
    pending = producer.flush(12)
    if first_error is not None:
        raise KafkaException(first_error)
    if pending or acknowledged != submitted:
        raise KafkaException(f"publication uncertain, {acknowledged} confirmed")
    return acknowledged
