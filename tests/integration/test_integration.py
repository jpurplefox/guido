import pytest

from contextlib import contextmanager

from guido import Guido, KafkaService, Message, KafkaConfig
from guido.messages import CommitData


@pytest.fixture
def kafka_service():
    service = KafkaService(
        KafkaConfig(
            bootstrap_servers="localhost:29092",
            group_id="test",
            consumer_timeout_ms=300000,
        )
    )
    yield service
    service.get_consumer().close()


@pytest.fixture
def message():
    return Message("foo", {"bar": "baz"})


@pytest.fixture
def produced_message(kafka_service, message):
    return kafka_service.produce(message)


@contextmanager
def safe_topic(kafka_service, topic):
    end = kafka_service.get_end_offset(topic)
    committed = kafka_service.get_last_committed(topic)
    if end != committed:
        kafka_service.commit(CommitData(topic=topic, offset=end))
    yield


def test_produce_message(kafka_service, message):
    with safe_topic(kafka_service, message.topic):
        produced_message = kafka_service.produce(message)

        assert message.value == produced_message.value
        assert message.topic == produced_message.topic

        kafka_service.subscribe([message.topic])
        assert next(kafka_service.get_messages()) == produced_message


def test_commit_message(kafka_service, message):
    with safe_topic(kafka_service, message.topic):
        assert kafka_service.get_pending_messages(message.topic) == 0
        kafka_service.produce(message)

        kafka_service.subscribe([message.topic])
        next(kafka_service.get_messages())

        assert kafka_service.get_pending_messages(message.topic) == 1

        kafka_service.commit()

        assert kafka_service.get_pending_messages(message.topic) == 0


def test_pending_messages_on_unexistant_topic(kafka_service):
    assert kafka_service.get_pending_messages("does_not_exist", 1) == 0
