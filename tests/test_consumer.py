"""Unit tests for the source guard's broker-error classification and propagation retry."""
from uuid import UUID, uuid4

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from usage_stream import consumer
from usage_stream.consumer import (
    KafkaPartition,
    TopicUnavailable,
    _check_broker_error,
    _retry_propagation,
)
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
        with pytest.raises(TopicUnavailable, match="topic or offset unavailable"):
            _check_broker_error(KafkaError(code))


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


def test_check_retries_leadership_lag_during_watermark_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_clock(monkeypatch)
    partition = object.__new__(KafkaPartition)
    watermarks = _FlakyWatermarks(failures=2, code=KafkaError.NOT_LEADER_FOR_PARTITION)
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


def test_check_fails_closed_when_leadership_never_settles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_clock(monkeypatch)
    partition = object.__new__(KafkaPartition)
    watermarks = _FailingWatermarks(KafkaError.LEADER_NOT_AVAILABLE)
    topic_id = uuid4()
    monkeypatch.setattr(partition, "consumer", watermarks, raising=False)
    monkeypatch.setattr(partition, "partition", TopicPartition("usage-test", 0), raising=False)
    monkeypatch.setattr(partition, "_describe_topic_id", lambda: topic_id, raising=False)
    with pytest.raises(TopicUnavailable):
        partition.check(topic_id, 3)
    assert watermarks.calls == 6  # attempts at t = 0, 1, 2, 3, 4, 5 with a 1s pause and a 5s deadline
