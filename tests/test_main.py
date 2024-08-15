import pytest

from guido import Guido
from guido.messages import OnMemoryService, Message


@pytest.fixture
def on_memory_service():
    return OnMemoryService()


@pytest.fixture
def app(on_memory_service):
    return Guido(on_memory_service)


@pytest.fixture
def produced_message(on_memory_service):
    message = Message("a_topic", {"data": "test"})
    produced_message = on_memory_service.produce(message)
    return produced_message


def test_consume_a_message(app, on_memory_service, produced_message):
    processed = []

    @app.subscribe(produced_message.topic)
    def process_message(message):
        processed.append(message)

    app.run()

    assert (
        on_memory_service.get_last_commited(produced_message.topic)
        == produced_message.offset
    )
    assert processed == [produced_message.value]
