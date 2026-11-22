"""Publish fixture and dated archive deliveries to one Kafka partition."""
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from importlib.resources import files
from pathlib import Path
from time import monotonic
from typing import NoReturn, cast

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from .events import MAX_PAYLOAD_BYTES, InvalidEvent, decode_event

TOPIC_CONFIG = {
    "cleanup.policy": "delete",
    "retention.ms": "604800000",
    "retention.bytes": "-1",
    "message.timestamp.type": "LogAppendTime",
}
MAX_ARCHIVE_BYTES = 16 * 1024 * 1024
MAX_ARCHIVE_ROWS = 50_000
MAX_MANIFEST_BYTES = 4096
FLUSH_SECONDS = 30.0
MANIFEST_KEYS = {"day", "sha256", "row_count"}
DAY = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
SHA256 = re.compile(r"[0-9a-f]{64}")


class ArchiveError(ValueError):
    pass


class PublicationUncertain(RuntimeError):
    """Publication did not confirm every row. `confirmed` is a lower bound on broker writes."""

    def __init__(self, confirmed: int, requested: int, cause: str) -> None:
        super().__init__(f"{confirmed} of {requested} deliveries confirmed ({cause})")
        self.confirmed = confirmed
        self.requested = requested


@dataclass(frozen=True, slots=True)
class Manifest:
    day: date
    sha256: str
    row_count: int


def fixture_deliveries() -> list[tuple[bytes, bytes]]:
    fixture = files("usage_stream").joinpath("fixtures", "deliveries.json")
    return [
        (event["event_id"].encode(), json.dumps(event, separators=(",", ":")).encode())
        for event in json.loads(fixture.read_text(encoding="utf-8"))
    ]


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    fields = dict(pairs)
    if len(fields) != len(pairs):
        raise ArchiveError("manifest repeats a key")
    return fields


def _reject_constant(_: str) -> NoReturn:
    raise ArchiveError("manifest is not a UTF-8 JSON object")


def read_manifest(path: Path) -> Manifest:
    with open(path, "rb") as stream:
        raw = stream.read(MAX_MANIFEST_BYTES + 1)
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ArchiveError("manifest is too large")
    try:
        parsed: object = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_pairs, parse_constant=_reject_constant,
        )
    except ArchiveError:
        raise
    except (ValueError, RecursionError) as exc:
        raise ArchiveError("manifest is not a UTF-8 JSON object") from exc
    if not isinstance(parsed, dict):
        raise ArchiveError("manifest is not a UTF-8 JSON object")
    fields = cast(dict[str, object], parsed)
    if fields.keys() != MANIFEST_KEYS:
        raise ArchiveError("manifest must have exactly day, sha256, and row_count")
    day, digest, row_count = fields["day"], fields["sha256"], fields["row_count"]
    try:
        if not isinstance(day, str) or not DAY.fullmatch(day):
            raise ValueError
        parsed_day = date.fromisoformat(day)
    except ValueError as exc:
        raise ArchiveError("manifest day must be an ISO UTC date") from exc
    if not isinstance(digest, str) or not SHA256.fullmatch(digest):
        raise ArchiveError("manifest sha256 must be 64 lowercase hex characters")
    if type(row_count) is not int or not 1 <= row_count <= MAX_ARCHIVE_ROWS:
        raise ArchiveError("manifest row_count must be an integer from 1 to 50,000")
    return Manifest(parsed_day, digest, row_count)


def _row_key(line: bytes) -> bytes | None:
    try:
        event_id = json.loads(line).get("event_id")
    except (ValueError, AttributeError, RecursionError):
        return None
    return event_id.encode("utf-8") if isinstance(event_id, str) else None


def read_archive(file: Path, manifest_file: Path) -> list[tuple[bytes, bytes]]:
    """Read and validate a dated archive. Return its rows as (key, payload) in file order.

    The returned bytes are the only copy used for publication, so later changes to the
    file cannot reach Kafka.
    """
    manifest = read_manifest(manifest_file)
    with open(file, "rb") as stream:
        data = stream.read(MAX_ARCHIVE_BYTES + 1)
    if len(data) > MAX_ARCHIVE_BYTES:
        raise ArchiveError("archive is larger than 16 MiB")
    if sha256(data).hexdigest() != manifest.sha256:
        raise ArchiveError("archive checksum does not match the manifest")
    body = data[:-1] if data.endswith(b"\n") else data
    if b"\r" in body:
        raise ArchiveError("archive must use LF row separators")
    lines = body.split(b"\n")
    for number, line in enumerate(lines, 1):
        if not line:
            raise ArchiveError(f"row {number} is blank")
    if len(lines) > MAX_ARCHIVE_ROWS:
        raise ArchiveError("archive has more than 50,000 rows")
    if len(lines) != manifest.row_count:
        raise ArchiveError(f"row count {len(lines)} does not match the manifest")
    deliveries = []
    for number, line in enumerate(lines, 1):
        if len(line) > MAX_PAYLOAD_BYTES:
            raise ArchiveError(f"row {number} is larger than 16,384 bytes")
        key = _row_key(line)
        try:
            event = decode_event(key, line)
        except InvalidEvent as exc:
            raise ArchiveError(f"row {number} is invalid ({exc.reason})") from exc
        if event.event_time.date() != manifest.day:
            raise ArchiveError(f"row {number} is outside {manifest.day.isoformat()}")
        assert key is not None
        deliveries.append((key, line))
    return deliveries


def publish(broker: str, topic: str, deliveries: Sequence[tuple[bytes, bytes]]) -> int:
    """Send rows in order to partition 0 and return the confirmed count.

    Raise PublicationUncertain unless every row has a successful delivery callback and
    the bounded flush leaves nothing pending.
    """
    producer = Producer({
        "bootstrap.servers": broker,
        "allow.auto.create.topics": False,
        "enable.idempotence": True,
        "message.timeout.ms": int(FLUSH_SECONDS * 1000) - 5_000,
    })
    requested = len(deliveries)
    confirmed = 0
    first_error: KafkaError | None = None

    def complete(error: KafkaError | None, _message: Message) -> None:
        nonlocal confirmed, first_error
        if error is None:
            confirmed += 1
        elif first_error is None:
            first_error = error

    deadline = monotonic() + FLUSH_SECONDS
    try:
        for key, payload in deliveries:
            while True:
                try:
                    producer.produce(topic, value=payload, key=key, partition=0, on_delivery=complete)
                    break
                except BufferError:
                    if monotonic() > deadline:
                        raise
                    producer.poll(0.1)
            producer.poll(0)
    except (BufferError, KafkaException) as exc:
        producer.flush(max(0.0, deadline - monotonic()))
        raise PublicationUncertain(confirmed, requested, str(exc)) from exc
    pending = producer.flush(max(0.0, deadline - monotonic()))
    if first_error is not None:
        raise PublicationUncertain(confirmed, requested, first_error.str())
    if pending or confirmed != requested:
        raise PublicationUncertain(confirmed, requested, f"{pending} still pending")
    return confirmed
