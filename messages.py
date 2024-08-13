import json

from kafka import KafkaConsumer

from typing import Callable, Protocol
from dataclasses import dataclass


@dataclass
class Message:
    topic: str
    value: str


class MessagesService(Protocol):
    def subscribe(self, topics: list[str]):
        ...

    def commit(self):
        ...

    def get_messages(self) -> list[Message]:
        ...


class KafkaService:
    def __init__(self, bootstrap_servers: str, group_id: str):
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers, group_id=group_id
        )

    def subscribe(self, topics: list[str]):
        self.consumer.subscribe(topics)

    def commit(self):
        self.consumer.commit()

    def get_messages(self) -> list[Message]:
        for message in self.consumer:
            yield Message(
                topic=message.topic, value=json.loads(message.value.decode("utf-8"))
            )


class OnMemoryService:
    def __init__(self):
        self.messages = []
        self.subscripted = []
        self.commited_messages = 0

    def subscribe(self, topics: list[str]):
        self.subscripted = topics

    def commit(self):
        self.commited_messages += 1

    def get_messages(self) -> list[Message]:
        for message in self.messages:
            if message.topic in self.subscripted:
                yield message

    def produce(self, message: Message):
        self.messages.append(message)
