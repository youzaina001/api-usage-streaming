from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Literal
from uuid import UUID

from psycopg import Connection, IsolationLevel
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow

from .events import InvalidEvent, decode_event

Outcome = Literal["accepted", "duplicate", "conflict", "invalid"]


class SourceGuardError(RuntimeError):
    pass


class StaleCheckpoint(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Checkpoint:
    topic_id: UUID
    partition: int
    next_offset: int


@dataclass(frozen=True, slots=True)
class Delivery:
    topic_id: UUID
    partition: int
    offset: int
    key: bytes | None
    payload: bytes | None


class Storage:
    def __init__(self, conn: Connection[TupleRow]) -> None:
        if not conn.autocommit:
            raise ValueError("storage requires a dedicated autocommit connection")
        conn.isolation_level = IsolationLevel.READ_COMMITTED
        self.conn = conn

    def read_checkpoint(self, pipeline_id: str) -> Checkpoint | None:
        row = self.conn.execute("""
            SELECT topic_id, partition, next_offset
            FROM ops.consumer_checkpoint WHERE pipeline_id = %s
        """, (pipeline_id,)).fetchone()
        return Checkpoint(*row) if row is not None else None

    def bind_checkpoint(self, pipeline_id: str, topic_id: UUID) -> Checkpoint:
        self.conn.execute("""
            INSERT INTO ops.consumer_checkpoint (pipeline_id, topic_id, partition, next_offset)
            VALUES (%s, %s, 0, 0) ON CONFLICT (pipeline_id) DO NOTHING
        """, (pipeline_id, topic_id))
        checkpoint = self.read_checkpoint(pipeline_id)
        if checkpoint is None or checkpoint.topic_id != topic_id:
            raise SourceGuardError("checkpoint belongs to a different topic")
        return checkpoint

    def apply_batch(
        self, pipeline_id: str, expected_topic_id: UUID, expected_partition: int,
        expected_next_offset: int, deliveries: Sequence[Delivery],
    ) -> int:
        if self.conn.info.transaction_status != TransactionStatus.IDLE:
            raise ValueError("apply_batch must own the transaction")
        with self.conn.transaction():
            row = self.conn.execute("""
                SELECT topic_id, partition, next_offset
                FROM ops.consumer_checkpoint WHERE pipeline_id = %s FOR UPDATE
            """, (pipeline_id,)).fetchone()
            if row is None or row[:2] != (expected_topic_id, expected_partition):
                raise SourceGuardError("batch does not match the bound topic and partition")
            if row[2] != expected_next_offset:
                raise StaleCheckpoint("saved offset changed")

            next_offset = expected_next_offset
            for delivery in deliveries:
                if (
                    delivery.topic_id != expected_topic_id
                    or delivery.partition != expected_partition
                    or delivery.offset < next_offset
                ):
                    raise SourceGuardError("batch offsets or source coordinates are invalid")
                next_offset = delivery.offset + 1
                self._record_delivery(delivery)
            if deliveries:
                self.conn.execute("""
                    UPDATE ops.consumer_checkpoint SET next_offset = %s WHERE pipeline_id = %s
                """, (next_offset, pipeline_id))
        return next_offset

    def _record_delivery(self, delivery: Delivery) -> None:
        coordinates = (delivery.topic_id, delivery.partition, delivery.offset)
        digest = sha256(delivery.payload).digest() if delivery.payload is not None else None
        try:
            event = decode_event(delivery.key, delivery.payload)
        except InvalidEvent as exc:
            self.conn.execute("""
                INSERT INTO ops.delivery (topic_id, partition, "offset", outcome,
                                          reason, event_id, payload_sha256)
                VALUES (%s, %s, %s, 'invalid', %s, %s, %s)
            """, (*coordinates, exc.reason, exc.parsed_event_id, digest))
            return

        values = (event.event_time, event.project_id, event.model_id,
                  event.input_tokens, event.output_tokens)
        existing = self.conn.execute("""
            SELECT event_time = %s, project_id, model_id, input_tokens, output_tokens
            FROM warehouse.usage_event WHERE event_id = %s
        """, (event.event_time, event.event_id)).fetchone()
        outcome: Outcome
        if existing is None:
            outcome = "accepted"
        else:
            outcome = "duplicate" if existing == (True, *values[1:]) else "conflict"
        self.conn.execute("""
            INSERT INTO ops.delivery (topic_id, partition, "offset", outcome,
                                      reason, event_id, payload_sha256)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            *coordinates, outcome, "changed_event" if outcome == "conflict" else None,
            event.event_id, digest,
        ))
        if outcome == "accepted":
            self.conn.execute("""
                INSERT INTO warehouse.usage_event (event_id, event_time, project_id, model_id,
                                                   input_tokens, output_tokens,
                                                   topic_id, partition, "offset")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (event.event_id, *values, *coordinates))
