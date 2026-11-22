import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from usage_stream.events import InvalidEvent, UsageEvent, decode_event

EVENT_ID = "12345678-1234-4234-8234-123456789abc"
KEY = EVENT_ID.encode()
EVENT: dict[str, object] = {
    "schema_version": 1,
    "event_id": EVENT_ID,
    "event_time": "2026-11-22T00:30:00.123456+01:00",
    "project_id": "project-a",
    "model_id": "model-a",
    "input_tokens": 0,
    "output_tokens": 2_147_483_647,
}
PAYLOAD = json.dumps(EVENT).encode()


def test_decode_normalizes_instant_and_accepts_inclusive_integer_limits() -> None:
    assert decode_event(KEY, PAYLOAD) == UsageEvent(
        UUID(EVENT_ID), datetime(2026, 11, 21, 23, 30, 0, 123456, tzinfo=UTC),
        "project-a", "model-a", 0, 2_147_483_647,
    )


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("event_id", EVENT_ID.upper(), "invalid_event_id"),
        ("event_id", "not-a-uuid", "invalid_event_id"),
        ("schema_version", 2, "invalid_schema_version"),
        ("schema_version", True, "invalid_schema_version"),
        ("schema_version", 1.0, "invalid_schema_version"),
        ("input_tokens", True, "invalid_tokens"),
        ("input_tokens", -1, "invalid_tokens"),
        ("output_tokens", 2_147_483_648, "invalid_tokens"),
        ("output_tokens", 1.0, "invalid_tokens"),
        ("project_id", "é", "invalid_identifier"),
        ("project_id", "-project", "invalid_identifier"),
        ("model_id", "a" * 65, "invalid_identifier"),
        ("model_id", "", "invalid_identifier"),
        ("event_time", "2026-11-22T00:30:00", "invalid_event_time"),
        ("event_time", "2026-11-22T00:30:60Z", "invalid_event_time"),
        ("event_time", "2026-11-22T00:30:00.1234567Z", "invalid_event_time"),
        ("event_time", "2026-02-31T00:30:00Z", "invalid_event_time"),
        ("event_time", "2026-11-22T00:30:00+00:60", "invalid_event_time"),
        ("unknown", 0, "invalid_fields"),
    ],
)
def test_rejects_invalid_fields(field: str, value: object, reason: str) -> None:
    with pytest.raises(InvalidEvent) as failure:
        decode_event(KEY, json.dumps(EVENT | {field: value}).encode())
    assert failure.value.reason == reason
    assert failure.value.parsed_event_id == (None if field == "event_id" else UUID(EVENT_ID))


@pytest.mark.parametrize(("payload", "reason"), [
    (None, "null_payload"),
    (b"\xff", "invalid_json"),
    (b'{"schema_version": NaN}', "invalid_json"),
    (b"[]", "invalid_fields"),
    (b"{}", "invalid_fields"),
    (b'{"schema_version": 1, ' + PAYLOAD[1:], "duplicate_json_key"),
])
def test_rejects_invalid_payloads(payload: bytes | None, reason: str) -> None:
    with pytest.raises(InvalidEvent) as failure:
        decode_event(KEY, payload)
    assert (failure.value.reason, failure.value.parsed_event_id) == (reason, None)


@pytest.mark.parametrize("key", [None, b"wrong", KEY.upper()])
def test_requires_canonical_matching_kafka_key(key: bytes | None) -> None:
    with pytest.raises(InvalidEvent) as failure:
        decode_event(key, PAYLOAD)
    assert (failure.value.reason, failure.value.parsed_event_id) == ("key_mismatch", UUID(EVENT_ID))


def test_payload_byte_limit_is_inclusive() -> None:
    boundary = PAYLOAD.ljust(16_384, b" ")
    assert decode_event(KEY, boundary).event_id == UUID(EVENT_ID)
    with pytest.raises(InvalidEvent) as failure:
        decode_event(KEY, boundary + b" ")
    assert (failure.value.reason, failure.value.parsed_event_id) == ("payload_too_large", None)
