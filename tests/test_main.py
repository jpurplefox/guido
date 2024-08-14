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
    on_memory_service.produce(message)
    return message


def test_app(app, on_memory_service, produced_message):
    processed = []

    @app.subscribe(produced_message.topic)
    def process_message(message):
        processed.append(message)

    app.run()

    assert on_memory_service.commited_messages == 1
    assert processed == [produced_message.value]
