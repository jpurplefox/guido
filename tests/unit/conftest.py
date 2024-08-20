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
def topic():
    return "foo"


@pytest.fixture
def message(topic):
    return Message(topic, {"data": "test"})


@pytest.fixture
def produced_message(app, message):
    return app.produce(message)


@pytest.fixture
def processed_messages(app, topic):
    processed = []

    @app.subscribe(topic)
    def process_message(message):
        processed.append(message)

    return processed
