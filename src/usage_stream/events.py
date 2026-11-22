import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import NoReturn, cast
from uuid import UUID

MAX_PAYLOAD_BYTES = 16_384
FIELDS = {
    "schema_version", "event_id", "event_time", "project_id", "model_id",
    "input_tokens", "output_tokens",
}
IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt](?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
    r"(?:\.[0-9]{1,6})?(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])",
)


@dataclass(frozen=True, slots=True)
class UsageEvent:
    event_id: UUID
    event_time: datetime
    project_id: str
    model_id: str
    input_tokens: int
    output_tokens: int


class InvalidEvent(ValueError):
    def __init__(self, reason: str, parsed_event_id: UUID | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.parsed_event_id = parsed_event_id


def _unique_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    fields = dict(pairs)
    if len(fields) != len(pairs):
        raise InvalidEvent("duplicate_json_key")
    return fields


def _reject_constant(_: str) -> NoReturn:
    raise InvalidEvent("invalid_json")


def _event_id(value: object) -> UUID | None:
    if not isinstance(value, str) or len(value) != 36:
        return None
    try:
        parsed = UUID(value)
    except ValueError:
        return None
    return parsed if str(parsed) == value else None


def decode_event(key: bytes | None, payload: bytes | None) -> UsageEvent:
    if payload is None:
        raise InvalidEvent("null_payload")
    if len(payload) > MAX_PAYLOAD_BYTES:
        raise InvalidEvent("payload_too_large")
    try:
        parsed: object = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_reject_constant,
        )
    except InvalidEvent:
        raise
    except (ValueError, RecursionError) as exc:
        raise InvalidEvent("invalid_json") from exc
    if not isinstance(parsed, dict):
        raise InvalidEvent("invalid_fields")
    fields = cast(dict[str, object], parsed)
    event_id = _event_id(fields.get("event_id"))
    if fields.keys() != FIELDS:
        raise InvalidEvent("invalid_fields", event_id)
    if event_id is None:
        raise InvalidEvent("invalid_event_id")
    if key != str(event_id).encode("ascii"):
        raise InvalidEvent("key_mismatch", event_id)
    version = fields["schema_version"]
    if type(version) is not int or version != 1:
        raise InvalidEvent("invalid_schema_version", event_id)
    stamp = fields["event_time"]
    if not isinstance(stamp, str) or not TIMESTAMP.fullmatch(stamp):
        raise InvalidEvent("invalid_event_time", event_id)
    try:
        event_time = datetime.fromisoformat(stamp.replace("z", "Z")).astimezone(UTC)
    except (ValueError, OverflowError) as exc:
        raise InvalidEvent("invalid_event_time", event_id) from exc
    project, model = fields["project_id"], fields["model_id"]
    if (
        not isinstance(project, str) or not IDENTIFIER.fullmatch(project)
        or not isinstance(model, str) or not IDENTIFIER.fullmatch(model)
    ):
        raise InvalidEvent("invalid_identifier", event_id)
    inputs, outputs = fields["input_tokens"], fields["output_tokens"]
    if (
        type(inputs) is not int or not 0 <= inputs <= 2_147_483_647
        or type(outputs) is not int or not 0 <= outputs <= 2_147_483_647
    ):
        raise InvalidEvent("invalid_tokens", event_id)
    return UsageEvent(event_id, event_time, project, model, inputs, outputs)
