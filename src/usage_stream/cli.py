import argparse
import os
import sys
from math import isfinite

import psycopg
from confluent_kafka import KafkaException
from psycopg.conninfo import make_conninfo

from .consumer import IngestionTimeout, KafkaPartition, consume, setup
from .source import fixture_deliveries, publish
from .storage import SourceGuardError, StaleCheckpoint, Storage


def _demo_dsn(role: str, password: str) -> str:
    return make_conninfo(
        host="127.0.0.1", port=os.environ.get("USAGE_POSTGRES_PORT", "15432"),
        dbname="usage", user=role, password=password,
    )


def main() -> int:
    parser = argparse.ArgumentParser(prog="usage-stream")
    kafka_port = os.environ.get("USAGE_KAFKA_PORT") or "19092"
    parser.add_argument("--broker", default=os.environ.get("USAGE_BROKER", f"127.0.0.1:{kafka_port}"))
    parser.add_argument("--topic", default=os.environ.get("USAGE_TOPIC", "api-usage"))
    parser.add_argument("--pipeline", default=os.environ.get("USAGE_PIPELINE", "usage"))
    parser.add_argument("--installer-dsn", default=os.environ.get(
        "USAGE_INSTALLER_DSN", _demo_dsn("installer", "usage-installer-local"),
    ))
    parser.add_argument("--consumer-dsn", default=os.environ.get(
        "USAGE_CONSUMER_DSN", _demo_dsn("usage_consumer", "usage-consumer-local"),
    ))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("setup")
    commands.add_parser("publish-fixture")
    consume_args = commands.add_parser("consume")
    consume_args.add_argument("--until-high-watermark", action="store_true")
    consume_args.add_argument("--timeout-seconds", type=float)
    consume_args.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()
    if args.command == "consume":
        if not 1 <= args.batch_size <= 1000:
            parser.error("--batch-size must be between 1 and 1000")
        if args.timeout_seconds is not None and (
            not isfinite(args.timeout_seconds) or args.timeout_seconds <= 0
        ):
            parser.error("--timeout-seconds must be finite and positive")
        if args.until_high_watermark and args.timeout_seconds is None:
            args.timeout_seconds = 30.0

    try:
        if args.command == "publish-fixture":
            acknowledged = publish(args.broker, args.topic, fixture_deliveries())
            print(f"Acknowledged {acknowledged} deliveries")
            return 0

        dsn = args.installer_dsn if args.command == "setup" else args.consumer_dsn
        with psycopg.connect(
            dsn, autocommit=True, connect_timeout=5,
            options="-c statement_timeout=5000 -c lock_timeout=5000",
        ) as conn:
            storage = Storage(conn)
            source = KafkaPartition(args.broker, args.topic, args.pipeline)
            try:
                if args.command == "setup":
                    next_offset = setup(source, storage, args.pipeline).next_offset
                else:
                    next_offset = consume(
                        source, storage, args.pipeline, args.batch_size,
                        args.until_high_watermark, args.timeout_seconds,
                    )
            finally:
                source.close()
        print(f"Next offset {next_offset}")
    except SourceGuardError as exc:
        print(f"Source guard: {exc}", file=sys.stderr)
        return 3
    except StaleCheckpoint as exc:
        print(f"Stale checkpoint: {exc}", file=sys.stderr)
        return 4
    except KafkaException as exc:
        print(f"Kafka operation failed: {exc}", file=sys.stderr)
        return 5
    except (psycopg.Error, TimeoutError) as exc:
        print(f"Operation failed: {type(exc).__name__}", file=sys.stderr)
        return 5
    except IngestionTimeout as exc:
        print(str(exc), file=sys.stderr)
        return 6
    except KeyboardInterrupt:
        return 130
    return 0
