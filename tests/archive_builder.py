"""Build dated test archives with exact sizes, independently of the backfill code."""
import json
from hashlib import sha256
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

FIXTURES = Path(__file__).resolve().parent / "fixtures"
ARCHIVE = FIXTURES / "archive-2026-11-21.jsonl"
MANIFEST = FIXTURES / "archive-2026-11-21.manifest.json"
DAY = "2026-11-21"
EVENT_C = "e944f6d9-c1a2-5cb0-a047-a48f2f069579"
# A through C come from the unit 1 fixture. D extends the seed 42 fixture with this name.
EVENT_D = str(uuid5(NAMESPACE_URL, "api-usage-streaming/seed-42/D"))
MIB_16 = 16 * 1024 * 1024


def event(event_id: str, event_time: str, input_tokens: int, output_tokens: int) -> dict[str, object]:
    return {
        "schema_version": 1, "event_id": event_id, "event_time": event_time,
        "project_id": "project-a", "model_id": "model-a",
        "input_tokens": input_tokens, "output_tokens": output_tokens,
    }


def row(fields: dict[str, object], width: int | None = None) -> bytes:
    """Encode one JSON row, padded with insignificant spaces to exactly `width` bytes."""
    line = json.dumps(fields, separators=(",", ":")).encode()
    if width is not None:
        assert len(line) <= width
        line = line[:-1] + b" " * (width - len(line)) + b"}"
    return line


def bulk_rows(count: int, width: int | None = None) -> bytes:
    """Return `count` unique valid rows for DAY, each padded to `width` bytes, LF terminated."""
    return b"".join(
        row(event(str(uuid5(NAMESPACE_URL, f"api-usage-streaming/bulk/{index}")),
                  f"{DAY}T12:00:00Z", 1, 1), width) + b"\n"
        for index in range(count)
    )


def manifest(data: bytes, row_count: int, day: str = DAY) -> dict[str, object]:
    return {"day": day, "sha256": sha256(data).hexdigest(), "row_count": row_count}


def write_archive(
    directory: Path, data: bytes, fields: dict[str, object], name: str = "archive",
) -> tuple[Path, Path]:
    archive = directory / f"{name}.jsonl"
    manifest_path = directory / f"{name}.manifest.json"
    archive.write_bytes(data)
    manifest_path.write_text(json.dumps(fields), encoding="utf-8")
    return archive, manifest_path
