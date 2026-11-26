"""Broker-free checks that admin helpers keep their client alive until results arrive."""
from collections.abc import Callable

import pytest
from confluent_kafka import KafkaError, KafkaException

from .conftest import Demo
from .helpers import delete_before, replace_topic, topic_id

UNREACHABLE = Demo("127.0.0.1:1", "unreachable-topic", "unused", "1")


@pytest.mark.parametrize("call", [
    pytest.param(lambda: topic_id(UNREACHABLE, timeout=1), id="topic_id"),
    pytest.param(lambda: replace_topic(UNREACHABLE, timeout=1), id="replace_topic"),
    pytest.param(lambda: delete_before(UNREACHABLE, 1, timeout=1), id="delete_before"),
])
def test_admin_helpers_wait_for_the_broker_instead_of_destroying_the_client(
    call: Callable[[], object],
) -> None:
    with pytest.raises(KafkaException) as raised:
        call()
    error = raised.value.args[0]
    assert isinstance(error, KafkaError)
    assert error.code() != KafkaError._DESTROY, error
