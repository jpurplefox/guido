import json

from kafka import KafkaConsumer, KafkaProducer

from typing import Callable, Protocol
from dataclasses import dataclass


@dataclass
class Message:
    topic: str
    value: dict


@dataclass
class ProducedMessage:
    topic: str
    value: dict
    offset: int


class MessagesService(Protocol):
    def subscribe(self, topics: list[str]):
        ...

    def commit(self):
        ...

    def get_messages(self) -> list[ProducedMessage]:
        ...

    def produce(self, message: Message) -> ProducedMessage:
        ...


class KafkaService:
    def __init__(self, bootstrap_servers: str, group_id: str):
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers, group_id=group_id
        )
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def subscribe(self, topics: list[str]):
        self.consumer.subscribe(topics)

    def commit(self):
        self.consumer.commit()

    def get_messages(self) -> list[ProducedMessage]:
        for message in self.consumer:
            yield Message(
                topic=message.topic, value=json.loads(message.value.decode("utf-8"))
            )

    def produce(self, message: Message) -> ProducedMessage:
        future = self.producer.send(
            message.topic, json.dumps(message.value).encode("utf-8")
        )
        result = future.get()
        return ProducedMessage(
            topic=message.topic, value=message.value, offset=result.offset
        )


class OnMemoryService:
    def __init__(self):
        self.messages = []
        self.subscripted = []
        self.commited_messages = {}
        self.consumed_messages = []

    def subscribe(self, topics: list[str]):
        self.subscripted = topics

    def commit(self):
        for topic in self.subscripted:
            self.commit_topic(topic)

    def commit_topic(self, topic: str):
        consumed_messages = [
            message for message in self.consumed_messages if message.topic == topic
        ]
        if consumed_messages:
            self.commited_messages[topic] = consumed_messages[-1].offset

    def get_messages(self) -> list[Message]:
        for message in self.messages:
            if message.topic in self.subscripted:
                self.consumed_messages.append(message)
                yield message

    def get_next_offset(self, topic: str) -> int:
        messages_in_topic = [
            message for message in self.messages if message.topic == topic
        ]
        if not messages_in_topic:
            offset = 0
        else:
            offset = messages_in_topic[-1] + 1
        return offset

    def produce(self, message: Message) -> ProducedMessage:
        produced_message = ProducedMessage(
            topic=message.topic,
            value=message.value,
            offset=self.get_next_offset(message.topic),
        )
        self.messages.append(produced_message)
        return produced_message

    def get_last_commited(self, topic: str) -> int:
        try:
            last_commited = self.commited_messages[topic]
        except KeyError:
            last_commited = None
        return last_commited
