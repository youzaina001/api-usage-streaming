from base64 import urlsafe_b64decode
from collections.abc import Callable
from time import monotonic, sleep
from typing import cast
from uuid import UUID

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    TopicCollection,
    TopicPartition,
)
from confluent_kafka.admin import (  # type: ignore[attr-defined]
    AdminClient,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
    TopicDescription,
)

from . import _seams
from .source import TOPIC_CONFIG
from .storage import Checkpoint, Delivery, SourceGuardError, Storage

RPC_TIMEOUT = 5.0
POLL_WINDOW = 0.25
PROPAGATION_TIMEOUT = 5.0
PROPAGATION_PAUSE = 0.1
LEADERSHIP_ERRORS = {
    KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE,
}
PROPAGATION_ERRORS = {
    KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.UNKNOWN_TOPIC_ID,
    KafkaError._UNKNOWN_TOPIC, KafkaError._UNKNOWN_PARTITION,
}
SOURCE_ERRORS = PROPAGATION_ERRORS | {
    KafkaError._AUTO_OFFSET_RESET, KafkaError.OFFSET_OUT_OF_RANGE,
}


class IngestionTimeout(RuntimeError):
    pass


class TopicUnavailable(SourceGuardError):
    """The broker has not propagated the topic or leader yet, so retrying briefly is safe."""


def _check_broker_error(error: object, *, include_leadership: bool = False) -> None:
    if not isinstance(error, KafkaError):
        return
    code = error.code()
    if code in PROPAGATION_ERRORS or (include_leadership and code in LEADERSHIP_ERRORS):
        raise TopicUnavailable(f"topic or offset unavailable ({error.name()})")
    if code in SOURCE_ERRORS:
        raise SourceGuardError(f"topic or offset unavailable ({error.name()})")


def _retry_propagation[T](operation: Callable[[], T], timeout: float | None = None) -> T:
    """Absorb the broker metadata lag right after a topic is created.

    A topic that stays missing keeps raising TopicUnavailable until the deadline,
    so the guard still fails closed with exit 3; only the propagation race is absorbed.
    """
    deadline = monotonic() + (PROPAGATION_TIMEOUT if timeout is None else timeout)
    while True:
        try:
            return operation()
        except TopicUnavailable:
            if monotonic() >= deadline:
                raise
            sleep(PROPAGATION_PAUSE)


class KafkaPartition:
    def __init__(self, broker: str, topic: str, group_id: str) -> None:
        config: dict[str, str | int | float | bool] = {
            "bootstrap.servers": broker, "allow.auto.create.topics": False,
        }
        self.admin = AdminClient(config)
        self.consumer = Consumer(config | {
            "group.id": group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "error",
            "isolation.level": "read_committed",
        })
        self.topic = topic
        self.partition = TopicPartition(topic, 0)

    def close(self) -> None:
        self.consumer.close()

    def _topic_id(self) -> UUID:
        return _retry_propagation(self._describe_topic_id)

    def _describe_topic_id(self) -> UUID:
        try:
            future = self.admin.describe_topics(
                TopicCollection([self.topic]), request_timeout=RPC_TIMEOUT,
            )[self.topic]
            description = cast(TopicDescription, future.result(timeout=RPC_TIMEOUT + 1))
        except KafkaException as exc:
            _check_broker_error(exc.args[0], include_leadership=True)
            raise
        topic_id = UUID(bytes=urlsafe_b64decode(f"{description.topic_id}=="))
        if not topic_id.int or [part.id for part in description.partitions] != [0]:
            raise SourceGuardError("topic must have an identity and exactly partition 0")
        return topic_id

    def _configs(self, resource: ConfigResource) -> dict[str, ConfigEntry]:
        try:
            entries = self.admin.describe_configs(
                [resource], request_timeout=RPC_TIMEOUT,
            )[resource].result(timeout=RPC_TIMEOUT + 1)
        except KafkaException as exc:
            _check_broker_error(exc.args[0], include_leadership=True)
            raise
        return cast("dict[str, ConfigEntry]", entries)

    def _watermarks(self) -> tuple[int, int]:
        try:
            return self.consumer.get_watermark_offsets(
                self.partition, timeout=RPC_TIMEOUT, cached=False,
            )
        except KafkaException as exc:
            _check_broker_error(exc.args[0], include_leadership=True)
            raise

    def provision(self, *, create: bool) -> UUID:
        if create:
            try:
                self.admin.create_topics(
                    [NewTopic(self.topic, 1, 1, config=TOPIC_CONFIG)], request_timeout=RPC_TIMEOUT,
                )[self.topic].result(timeout=RPC_TIMEOUT + 1)
            except KafkaException as exc:
                error = exc.args[0]
                if not isinstance(error, KafkaError) or error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
        topic_id = self._topic_id()
        resource = ConfigResource(ResourceType.TOPIC, self.topic)
        entries = _retry_propagation(lambda: self._configs(resource))
        if any(entries[key].value != value for key, value in TOPIC_CONFIG.items()):
            raise SourceGuardError("topic retention or timestamp configuration is incompatible")
        return topic_id

    def check(self, topic_id: UUID, cursor: int, last_offset: int | None = None) -> int:
        low, high = _retry_propagation(self._watermarks)
        # On the standalone broker, describe after fetch and bounds before labeling the batch.
        if self._topic_id() != topic_id:
            raise SourceGuardError("topic identity changed")
        if not low <= cursor <= high:
            raise SourceGuardError(f"saved offset {cursor} is outside [{low}, {high}]")
        if last_offset is not None and not cursor <= last_offset < high:
            raise SourceGuardError("fetched offset is outside the checked bounds")
        return high

    def poll(self, topic_id: UUID, batch_size: int, timeout: float) -> list[Delivery]:
        deliveries = []
        for message in self.consumer.consume(batch_size, timeout):
            error = message.error()
            if error is not None:
                _check_broker_error(error)
                raise KafkaException(error)
            partition, offset = message.partition(), message.offset()
            if message.topic() != self.topic or partition != 0 or offset is None:
                raise SourceGuardError("message did not come from the assigned partition")
            deliveries.append(Delivery(topic_id, partition, offset, message.key(), message.value()))
        return deliveries


def setup(source: KafkaPartition, storage: Storage, pipeline_id: str) -> Checkpoint:
    checkpoint = storage.read_checkpoint(pipeline_id)
    topic_id = source.provision(create=checkpoint is None)
    if checkpoint is not None and checkpoint.topic_id != topic_id:
        raise SourceGuardError("checkpoint belongs to a different topic")
    cursor = checkpoint.next_offset if checkpoint else 0
    source.check(topic_id, cursor)
    return storage.bind_checkpoint(pipeline_id, topic_id)


def consume(
    source: KafkaPartition, storage: Storage, pipeline_id: str, batch_size: int,
    until_high_watermark: bool, timeout: float | None,
) -> int:
    deadline = monotonic() + timeout if timeout is not None else None
    checkpoint = storage.read_checkpoint(pipeline_id)
    if checkpoint is None:
        raise SourceGuardError("checkpoint is missing, run setup first")
    cursor, topic_id = checkpoint.next_offset, checkpoint.topic_id
    high = source.check(topic_id, cursor)
    target = high if until_high_watermark else None
    source.consumer.assign([TopicPartition(source.topic, 0, cursor)])
    idle_check = monotonic() + 1
    while target is None or cursor < target:
        remaining = deadline - monotonic() if deadline is not None else POLL_WINDOW
        if remaining <= 0:
            raise IngestionTimeout(f"consume timed out at offset {cursor}")
        deliveries = source.poll(topic_id, batch_size, min(POLL_WINDOW, remaining))
        if deliveries:
            _seams.reached("fetched", deliveries[-1].offset)
            source.check(topic_id, cursor, deliveries[-1].offset)
            if deadline is not None and monotonic() >= deadline:
                raise IngestionTimeout(f"consume timed out at offset {cursor}")
            if target is not None:
                deliveries = [delivery for delivery in deliveries if delivery.offset < target]
            cursor = storage.apply_batch(pipeline_id, topic_id, 0, cursor, deliveries)
            now = monotonic()
            if deadline is not None and now >= deadline:
                raise IngestionTimeout(f"consume timed out at offset {cursor}")
            idle_check = now + 1
        elif monotonic() >= idle_check:
            source.check(topic_id, cursor)
            idle_check = monotonic() + 1
    return cursor
