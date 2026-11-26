"""Unit tests for the source guard's broker-error classification and propagation retry."""
from base64 import urlsafe_b64encode
from concurrent.futures import Future
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import ConfigResource  # type: ignore[attr-defined]

from usage_stream import consumer
from usage_stream.consumer import (
    KafkaPartition,
    TopicUnavailable,
    _check_broker_error,
    _retry_propagation,
)
from usage_stream.source import TOPIC_CONFIG
from usage_stream.storage import SourceGuardError


def test_propagation_errors_are_retryable() -> None:
    assert issubclass(TopicUnavailable, SourceGuardError)
    for code in (
        KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.UNKNOWN_TOPIC_ID,
        KafkaError._UNKNOWN_TOPIC, KafkaError._UNKNOWN_PARTITION,
    ):
        with pytest.raises(TopicUnavailable, match="topic or offset unavailable"):
            _check_broker_error(KafkaError(code))


def test_leadership_errors_are_retryable() -> None:
    for code in (
        KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE,
    ):
        _check_broker_error(KafkaError(code))
        with pytest.raises(TopicUnavailable, match="topic or offset unavailable"):
            _check_broker_error(KafkaError(code), include_leadership=True)


def test_offset_errors_are_guard_failures_not_retries() -> None:
    for code in (KafkaError._AUTO_OFFSET_RESET, KafkaError.OFFSET_OUT_OF_RANGE):
        with pytest.raises(SourceGuardError) as raised:
            _check_broker_error(KafkaError(code))
        assert not isinstance(raised.value, TopicUnavailable)


def test_unrelated_broker_errors_pass_through() -> None:
    _check_broker_error(KafkaError(KafkaError._TIMED_OUT))
    _check_broker_error("not a kafka error")


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _patch_clock(monkeypatch: pytest.MonkeyPatch, pause: float = 1.0) -> _FakeClock:
    clock = _FakeClock()
    monkeypatch.setattr(consumer, "monotonic", clock.monotonic)
    monkeypatch.setattr(consumer, "sleep", clock.sleep)
    monkeypatch.setattr(consumer, "PROPAGATION_PAUSE", pause)
    return clock


def test_retry_propagation_absorbs_transient_unavailability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_clock(monkeypatch)
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TopicUnavailable("topic or offset unavailable (UNKNOWN_TOPIC_OR_PART)")
        return "ok"

    assert _retry_propagation(operation, timeout=5.0) == "ok"
    assert attempts == 3


def test_retry_propagation_fails_closed_when_topic_stays_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_clock(monkeypatch)
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        raise TopicUnavailable("topic or offset unavailable (UNKNOWN_TOPIC_OR_PART)")

    with pytest.raises(TopicUnavailable):
        _retry_propagation(operation, timeout=5.0)
    assert attempts == 6  # attempts at t = 0, 1, 2, 3, 4, 5 with a 1s pause and a 5s deadline


class _FlakyWatermarks:
    def __init__(
        self, failures: int, code: int = KafkaError.UNKNOWN_TOPIC_OR_PART,
    ) -> None:
        self.failures = failures
        self.code = code
        self.calls = 0

    def get_watermark_offsets(
        self, partition: TopicPartition, *, timeout: float, cached: bool,
    ) -> tuple[int, int]:
        self.calls += 1
        if self.calls <= self.failures:
            raise KafkaException(KafkaError(self.code))
        return 0, 7


def test_check_retries_watermark_lookup_after_topic_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_clock(monkeypatch)
    partition = object.__new__(KafkaPartition)
    watermarks = _FlakyWatermarks(failures=2)
    topic_id = uuid4()
    monkeypatch.setattr(partition, "consumer", watermarks, raising=False)
    monkeypatch.setattr(partition, "partition", TopicPartition("usage-test", 0), raising=False)
    monkeypatch.setattr(partition, "_describe_topic_id", lambda: topic_id, raising=False)
    assert partition.check(topic_id, 3) == 7
    assert watermarks.calls == 3


@pytest.mark.parametrize(
    "code",
    [KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE],
)
def test_check_retries_leadership_lag_during_watermark_lookup(
    monkeypatch: pytest.MonkeyPatch, code: int,
) -> None:
    _patch_clock(monkeypatch)
    partition = object.__new__(KafkaPartition)
    watermarks = _FlakyWatermarks(failures=2, code=code)
    topic_id = uuid4()
    monkeypatch.setattr(partition, "consumer", watermarks, raising=False)
    monkeypatch.setattr(partition, "partition", TopicPartition("usage-test", 0), raising=False)
    monkeypatch.setattr(partition, "_describe_topic_id", lambda: topic_id, raising=False)
    assert partition.check(topic_id, 3) == 7
    assert watermarks.calls == 3


def test_check_does_not_retry_topic_identity_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_clock(monkeypatch)
    partition = object.__new__(KafkaPartition)
    watermarks = _FlakyWatermarks(failures=0)
    describes: list[UUID] = []
    monkeypatch.setattr(partition, "consumer", watermarks, raising=False)
    monkeypatch.setattr(partition, "partition", TopicPartition("usage-test", 0), raising=False)

    def describe() -> UUID:
        describes.append(uuid4())
        return describes[-1]

    monkeypatch.setattr(partition, "_describe_topic_id", describe, raising=False)
    with pytest.raises(SourceGuardError, match="topic identity changed") as raised:
        partition.check(uuid4(), 3)
    assert not isinstance(raised.value, TopicUnavailable)
    assert len(describes) == 1
    assert watermarks.calls == 1


class _FailingWatermarks:
    def __init__(self, code: int) -> None:
        self.code = code
        self.calls = 0

    def get_watermark_offsets(
        self, partition: TopicPartition, *, timeout: float, cached: bool,
    ) -> tuple[int, int]:
        self.calls += 1
        raise KafkaException(KafkaError(self.code))


def test_check_does_not_retry_offset_out_of_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_clock(monkeypatch)
    partition = object.__new__(KafkaPartition)
    watermarks = _FailingWatermarks(KafkaError.OFFSET_OUT_OF_RANGE)
    monkeypatch.setattr(partition, "consumer", watermarks, raising=False)
    monkeypatch.setattr(partition, "partition", TopicPartition("usage-test", 0), raising=False)
    with pytest.raises(SourceGuardError, match="topic or offset unavailable") as raised:
        partition.check(uuid4(), 3)
    assert not isinstance(raised.value, TopicUnavailable)
    assert watermarks.calls == 1


@pytest.mark.parametrize(
    "code",
    [KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE],
)
def test_check_fails_closed_when_leadership_never_settles(
    monkeypatch: pytest.MonkeyPatch, code: int,
) -> None:
    _patch_clock(monkeypatch)
    partition = object.__new__(KafkaPartition)
    watermarks = _FailingWatermarks(code)
    topic_id = uuid4()
    monkeypatch.setattr(partition, "consumer", watermarks, raising=False)
    monkeypatch.setattr(partition, "partition", TopicPartition("usage-test", 0), raising=False)
    monkeypatch.setattr(partition, "_describe_topic_id", lambda: topic_id, raising=False)
    with pytest.raises(TopicUnavailable):
        partition.check(topic_id, 3)
    assert watermarks.calls == 6  # attempts at t = 0, 1, 2, 3, 4, 5 with a 1s pause and a 5s deadline


class _FakeMessage:
    def __init__(self, error: KafkaError) -> None:
        self._error = error

    def error(self) -> KafkaError:
        return self._error


class _FakePollConsumer:
    def __init__(self, code: int) -> None:
        self.code = code
        self.calls = 0

    def consume(self, batch_size: int, timeout: float) -> list[_FakeMessage]:
        self.calls += 1
        return [_FakeMessage(KafkaError(self.code))]


def test_poll_raises_kafka_exception_on_leadership_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for code in (KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE):
        partition = object.__new__(KafkaPartition)
        poll_consumer = _FakePollConsumer(code)
        monkeypatch.setattr(partition, "consumer", poll_consumer, raising=False)
        with pytest.raises(KafkaException) as raised:
            partition.poll(uuid4(), 10, 0.25)
        assert not isinstance(raised.value, (SourceGuardError, TopicUnavailable))
        assert raised.value.args[0].code() == code
        assert poll_consumer.calls == 1


def test_poll_still_raises_source_guard_error_on_source_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for code in (KafkaError.OFFSET_OUT_OF_RANGE, KafkaError.UNKNOWN_TOPIC_OR_PART):
        partition = object.__new__(KafkaPartition)
        poll_consumer = _FakePollConsumer(code)
        monkeypatch.setattr(partition, "consumer", poll_consumer, raising=False)
        with pytest.raises(SourceGuardError, match="topic or offset unavailable") as raised:
            partition.poll(uuid4(), 10, 0.25)
        assert isinstance(raised.value, TopicUnavailable) is (
            code == KafkaError.UNKNOWN_TOPIC_OR_PART
        )
        assert poll_consumer.calls == 1


class _FlakyAdmin:
    def __init__(
        self,
        topic_id: UUID,
        topic_errors: list[int],
        config_errors: list[int],
    ) -> None:
        self.topic_id = topic_id
        self.topic_errors = topic_errors
        self.config_errors = config_errors
        self.topic_calls = 0
        self.config_calls = 0

    def describe_topics(
        self, collection: object, *, request_timeout: float,
    ) -> dict[str, Future[object]]:
        self.topic_calls += 1
        future: Future[object] = Future()
        if self.topic_calls <= len(self.topic_errors):
            future.set_exception(KafkaException(KafkaError(self.topic_errors[self.topic_calls - 1])))
        else:
            encoded_id = urlsafe_b64encode(self.topic_id.bytes).decode("ascii").rstrip("=")
            future.set_result(
                SimpleNamespace(topic_id=encoded_id, partitions=[SimpleNamespace(id=0)]),
            )
        return {"usage-test": future}

    def describe_configs(
        self, resources: list[ConfigResource], *, request_timeout: float,
    ) -> dict[ConfigResource, Future[object]]:
        self.config_calls += 1
        future: Future[object] = Future()
        if self.config_calls <= len(self.config_errors):
            future.set_exception(
                KafkaException(KafkaError(self.config_errors[self.config_calls - 1])),
            )
        else:
            future.set_result(
                {key: SimpleNamespace(value=value) for key, value in TOPIC_CONFIG.items()},
            )
        return {resources[0]: future}


def test_provision_retries_transient_leadership_and_propagation_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_clock(monkeypatch)
    topic_id = uuid4()
    admin = _FlakyAdmin(
        topic_id=topic_id,
        topic_errors=[
            KafkaError.UNKNOWN_TOPIC_OR_PART,
            KafkaError.NOT_LEADER_FOR_PARTITION,
            KafkaError.LEADER_NOT_AVAILABLE,
        ],
        config_errors=[
            KafkaError.UNKNOWN_TOPIC_ID,
            KafkaError.LEADER_NOT_AVAILABLE,
            KafkaError.NOT_LEADER_FOR_PARTITION,
        ],
    )
    partition = object.__new__(KafkaPartition)
    monkeypatch.setattr(partition, "topic", "usage-test", raising=False)
    monkeypatch.setattr(partition, "admin", admin, raising=False)
    assert partition.provision(create=False) == topic_id
    assert admin.topic_calls == 4
    assert admin.config_calls == 4
