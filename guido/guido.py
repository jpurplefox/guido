import logging

from dataclasses import dataclass
from typing import Callable, Type, Iterator

from guido.messages import (
    MessagesService,
    Message,
    ProducedMessage,
    KafkaService,
    KafkaConfig,
    get_default_configuration,
    read_environ_configuration,
    combine_configuration,
)

logger = logging.getLogger("guido")


TSubscribedFunction = Callable[[dict], None]


@dataclass
class SubscribedFunction:
    func: TSubscribedFunction
    topic: str
    dead_letter_topic: str


class Guido:
    def __init__(
        self,
        config: KafkaConfig | None = None,
        messages_service_class: Type[MessagesService] = KafkaService,
        messages_service: MessagesService | None = None,
    ):
        self.config = config if config else KafkaConfig()
        self.messages_service = messages_service
        self.messages_service_class = messages_service_class

        self.subscribed_functions: list[SubscribedFunction] = []

    def get_default_dlt(self, topic: str):
        return f"{topic}_{self.get_group_id()}_dlt"

    def subscribe(
        self, topic: str, dead_letter_topic: str | None = None
    ) -> Callable[[TSubscribedFunction], TSubscribedFunction]:
        if not dead_letter_topic:
            dlt = self.get_default_dlt(topic)
        else:
            dlt = dead_letter_topic

        def decorator(func: TSubscribedFunction) -> TSubscribedFunction:
            function = SubscribedFunction(func=func, topic=topic, dead_letter_topic=dlt)
            self.subscribed_functions.append(function)
            return func

        return decorator

    def get_group_id(self) -> str:
        return self.get_messages_service().group_id

    def get_messages_service(self) -> MessagesService:
        default_config = read_environ_configuration()
        env_config = read_environ_configuration()
        config = combine_configuration(env_config, default_config)
        config = combine_configuration(self.config, config)
        if not self.messages_service:
            self.messages_service = self.messages_service_class(config)
        return self.messages_service

    def produce(self, message: Message) -> ProducedMessage:
        produced_message = self.get_messages_service().produce(message)
        return produced_message

    def get_last_committed(self, topic: str, partition: int = 0) -> int | None:
        return self.get_messages_service().get_last_committed(topic, partition)

    def get_pending_messages(self, topic: str, partition: int = 0) -> int:
        return self.get_messages_service().get_pending_messages(topic, partition)

    def get_messages(
        self, topics: list[str] | None = None
    ) -> Iterator[ProducedMessage]:
        if not topics:
            topics = [function.topic for function in self.subscribed_functions]
        logger.info(f"Subscribing to topics: {topics}")
        self.get_messages_service().subscribe(topics)
        return self.get_messages_service().get_messages()

    def search_function_for_topic(self, topic: str):
        return next(
            function
            for function in self.subscribed_functions
            if function.topic == topic
        )

    def process_message(self, message: ProducedMessage):
        subscribed_function = self.search_function_for_topic(message.topic)
        try:
            subscribed_function.func(message.value)
        except Exception as exc:
            logger.exception(f"Message {message} was not proccesed because of {exc}")
            dlt_message = Message(
                topic=subscribed_function.dead_letter_topic, value=message.value
            )
            logger.info(f"Producing dlt message: {dlt_message}")
            self.produce(dlt_message)

    def run(self):
        for msg in self.get_messages():
            logger.debug(f"Message received in topic {msg.topic}")
            self.process_message(msg)
            self.get_messages_service().commit()
