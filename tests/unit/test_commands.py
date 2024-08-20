import json
import pytest

from unittest.mock import patch

from guido.commands import parse_args, execute_command


def test_run_command(app, produced_message, processed_messages):
    with patch("sys.argv", ["guido", "test.app"]):
        args = parse_args()
    execute_command(app, args)

    assert app.get_last_committed(produced_message.topic) == produced_message.offset
    assert processed_messages == [produced_message.value]


def test_produce_command(app, message):
    with patch(
        "sys.argv",
        ["guido", "test.app", "produce", message.topic, json.dumps(message.value)],
    ):
        args = parse_args()
    execute_command(app, args)

    app.messages_service.subscribe([message.topic])

    produced_messages = app.messages_service.get_messages()
    produced_message = next(produced_messages)
    assert produced_message.topic == message.topic
    assert produced_message.value == message.value
    assert produced_message.offset == 0

    with pytest.raises(StopIteration):
        next(produced_messages)
