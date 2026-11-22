import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import TupleRow

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures"
TOPIC_CONFIG = {
    "cleanup.policy": "delete",
    "retention.ms": "604800000",
    "retention.bytes": "-1",
    "message.timestamp.type": "LogAppendTime",
}


@dataclass(frozen=True)
class Demo:
    broker: str
    topic: str
    db: str
    port: str

    def dsn(self, role: str, password: str) -> str:
        return make_conninfo(
            host="127.0.0.1", port=self.port, dbname=self.db,
            user=role, password=password, connect_timeout=5,
        )

    def report(self) -> psycopg.Connection[TupleRow]:
        return psycopg.connect(self.dsn("reporting_reader", "usage-reporting-local"))

    def admin(self) -> AdminClient:
        return AdminClient({"bootstrap.servers": self.broker, "allow.auto.create.topics": False})

    def new_topic(self, partitions: int = 1) -> None:
        admin = self.admin()
        admin.create_topics(
            [NewTopic(self.topic, partitions, 1, config=TOPIC_CONFIG)], request_timeout=15,
        )[self.topic].result(timeout=16)

    def cli_env(self) -> dict[str, str]:
        return {
            "USAGE_BROKER": self.broker,
            "USAGE_TOPIC": self.topic,
            "USAGE_PIPELINE": "usage",
            "USAGE_INSTALLER_DSN": self.dsn("installer", "usage-installer-local"),
            "USAGE_CONSUMER_DSN": self.dsn("usage_consumer", "usage-consumer-local"),
        }

    def cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(Path(sys.executable).with_name("usage-stream")), *args],
            env=os.environ | self.cli_env(),
            check=False, capture_output=True, text=True, timeout=45,
        )


@pytest.fixture
def demo() -> Demo:
    if os.environ.get("USAGE_TEST_INTEGRATION") != "1":
        pytest.skip("Set USAGE_TEST_INTEGRATION=1 against a disposable Compose stack.")
    key = uuid4().hex
    broker = os.environ.get("USAGE_BROKER", "127.0.0.1:19092")
    port = os.environ.get("USAGE_POSTGRES_PORT", "15432")
    instance = Demo(broker, f"usage-{key}", f"usage_{key}", port)
    bootstrap_dsn = instance.dsn("postgres", "usage-bootstrap-local")
    with psycopg.connect(bootstrap_dsn, dbname="postgres", autocommit=True) as conn:
        conn.execute(
            sql.SQL("CREATE DATABASE {} OWNER installer TEMPLATE template0").format(
                sql.Identifier(instance.db),
            ),
        )
    with psycopg.connect(bootstrap_dsn, autocommit=True) as conn:
        conn.execute((ROOT / "sql" / "001_usage.sql").read_text(encoding="utf-8"))
    return instance
