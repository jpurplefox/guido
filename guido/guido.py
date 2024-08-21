import logging

from typing import Callable, Type, Iterator
from guido.messages import (
    MessagesService,
    Message,
    ProducedMessage,
    KafkaService,
    KafkaConfig,
)

logger = logging.getLogger("guido")


TSubscribedFunction = Callable[[dict], None]


class ImproperlyConfigured(Exception):
    pass


class Guido:
    def __init__(
        self,
        config: KafkaConfig | None = None,
        messages_service_class: Type[MessagesService] = KafkaService,
        messages_service: MessagesService | None = None,
    ):
        self.config = config
        self.messages_service = messages_service
        self.messages_service_class = messages_service_class

        self._topics: dict[str, TSubscribedFunction] = {}

    def subscribe(
        self, topic: str
    ) -> Callable[[TSubscribedFunction], TSubscribedFunction]:
        def decorator(func: TSubscribedFunction) -> TSubscribedFunction:
            self._topics[topic] = func
            return func

        return decorator

    def get_group_id(self) -> str:
        return self.get_messages_service().group_id

    def get_messages_service(self) -> MessagesService:
        if not self.messages_service:
            if not self.config:
                raise ImproperlyConfigured()
            self.messages_service = self.messages_service_class(self.config)
        return self.messages_service

    def produce(self, message: Message) -> ProducedMessage:
        produced_message = self.get_messages_service().produce(message)
        return produced_message

    def get_last_committed(self, topic: str, partition: int = 0) -> int | None:
        return self.get_messages_service().get_last_committed(topic, partition)

    def get_pending_messages(self, topic: str, partition: int = 0) -> int:
        return self.get_messages_service().get_pending_messages(topic, partition)

    def get_messages(self, topics: list[str] | None = None) -> Iterator[ProducedMessage]:
        if not topics:
            topics = [key for key in self._topics.keys()]
        self.get_messages_service().subscribe(topics)
        return self.get_messages_service().get_messages()

    def run(self):
        logger.info(f"Subscribing to topics: {self._topics.keys()}")
        for msg in self.get_messages():
            logger.debug(f"Message received in topic {msg.topic}")
            try:
                self._topics[msg.topic](msg.value)
            except Exception as exc:
                logger.error(f"Message {msg} was not proccesed because of {exc}")
                dlt_message = Message(
                    topic=f"{msg.topic}_{self.get_group_id()}_dlt", value=msg.value
                )
                logger.info(f"Producing dlt message: {dlt_message}")
                self.produce(dlt_message)
            self.get_messages_service().commit()
