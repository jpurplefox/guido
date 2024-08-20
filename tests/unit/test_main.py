def test_produce_a_message(app, message):
    produced_message = app.produce(message)
    app.messages_service.subscribe([message.topic])

    assert produced_message.topic == message.topic
    assert produced_message.value == message.value
    assert produced_message.offset == 0
    assert list(app.messages_service.get_messages()) == [produced_message]


def test_consume_a_message(app, produced_message, processed_messages):
    app.run()

    assert app.get_last_committed(produced_message.topic) == produced_message.offset
    assert processed_messages == [produced_message.value]


def test_get_pending_messages(app, message):
    assert app.get_pending_messages(message.topic) == 0

    produced_message = app.produce(message)

    assert app.get_pending_messages(produced_message.topic) == 1
