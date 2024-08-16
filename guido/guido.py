import logging

from typing import Callable
from guido.messages import MessagesService, Message, ProducedMessage

logger = logging.getLogger("guido")


class Guido:
    def __init__(self, messages_service: MessagesService):
        self.messages_service = messages_service
        self._topics = {}

    def subscribe(self, topic: str):
        def decorator(func: Callable):
            self._topics[topic] = func

        return decorator

    def produce(self, message: Message) -> ProducedMessage:
        produced_message = self.messages_service.produce(message)
        return produced_message

    def get_last_committed(self, topic: str, partition: int = 0) -> int:
        return self.messages_service.get_last_committed(topic, partition)

    def run(self):
        logger.info(f"Subscribing to topics: {self._topics.keys()}")
        self.messages_service.subscribe(self._topics.keys())
        for msg in self.messages_service.get_messages():
            logger.debug(f"Message received in topic {msg.topic}")
            self._topics[msg.topic](msg.value)
            self.messages_service.commit()
