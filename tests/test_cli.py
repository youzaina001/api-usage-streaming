import sys
from collections.abc import Iterable

import pytest

from usage_stream import cli


@pytest.mark.parametrize(("port", "broker_env", "broker_arg", "expected"), [
    ("29092", None, (), "127.0.0.1:29092"),
    ("", None, (), "127.0.0.1:19092"),
    ("29092", "other:9092", (), "other:9092"),
    ("29092", "other:9092", ("--broker", "cli:9092"), "cli:9092"),
])
def test_publish_uses_configured_broker(
    monkeypatch: pytest.MonkeyPatch, port: str, broker_env: str | None,
    broker_arg: tuple[str, ...], expected: str,
) -> None:
    monkeypatch.setenv("USAGE_KAFKA_PORT", port)
    if broker_env is None:
        monkeypatch.delenv("USAGE_BROKER", raising=False)
    else:
        monkeypatch.setenv("USAGE_BROKER", broker_env)
    monkeypatch.setattr(sys, "argv", ["usage-stream", *broker_arg, "publish-fixture"])
    addresses: list[str] = []

    def publish(broker: str, _topic: str, deliveries: Iterable[tuple[bytes, bytes]]) -> int:
        addresses.append(broker)
        return len(list(deliveries))

    monkeypatch.setattr(cli, "publish", publish)
    assert cli.main() == 0
    assert addresses == [expected]
