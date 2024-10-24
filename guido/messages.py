import json
import os

from kafka import KafkaConsumer, KafkaProducer, TopicPartition  # type: ignore
from kafka.structs import OffsetAndMetadata  # type: ignore

from typing import Callable, Protocol, Iterator
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
    partition: int


@dataclass
class CommitData:
    topic: str
    offset: int
    partition: int = 0


@dataclass
class Config:
    group_id: str | None = None


@dataclass
class KafkaConfig(Config):
    bootstrap_servers: str | None = None
    consumer_timeout_ms: float = float("inf")
    security_protocol: str | None = None


class ImproperlyConfigured(Exception):
    pass


class MessagesService(Protocol):
    group_id: str

    def __init__(self, config: KafkaConfig):
        ...

    def subscribe(self, topics: list[str]):
        ...

    def commit(self, data: CommitData | None = None):
        ...

    def get_messages(self) -> Iterator[ProducedMessage]:
        ...

    def produce(self, message: Message) -> ProducedMessage:
        ...

    def get_last_committed(self, topic: str, partition: int = 0) -> int | None:
        ...

    def get_pending_messages(self, topic: str, partition: int = 0) -> int:
        ...


class KafkaService:
    group_id: str

    def __init__(
        self,
        config: KafkaConfig,
    ):
        self.config = config
        group_id = config.group_id
        if not self.config.bootstrap_servers or not group_id:
            raise ImproperlyConfigured()
        self.group_id = group_id

        self._consumer = None
        self._producer = None

    def get_consumer(self):
        if not self._consumer:
            self._consumer = KafkaConsumer(
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.group_id,
                consumer_timeout_ms=self.config.consumer_timeout_ms,
                security_protocol=self.config.security_protocol,
            )
        return self._consumer

    def get_producer(self):
        if not self._producer:
            self._producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                security_protocol=self.config.security_protocol,
            )
        return self._producer

    def subscribe(self, topics: list[str]):
        self.get_consumer().subscribe(topics)

    def commit(self, data: CommitData | None = None):
        if data:
            offsets = {
                TopicPartition(data.topic, data.partition): OffsetAndMetadata(
                    data.offset, None
                )
            }
        else:
            offsets = None
        self.get_consumer().commit(offsets)

    def get_messages(self) -> Iterator[ProducedMessage]:
        for message in self.get_consumer():
            yield ProducedMessage(
                topic=message.topic,
                value=json.loads(message.value.decode("utf-8")),
                offset=message.offset,
                partition=message.partition,
            )

    def produce(self, message: Message) -> ProducedMessage:
        future = self.get_producer().send(
            message.topic, json.dumps(message.value).encode("utf-8")
        )
        result = future.get()
        return ProducedMessage(
            topic=message.topic,
            value=message.value,
            offset=result.offset,
            partition=result.partition,
        )

    def get_last_committed(self, topic: str, partition: int = 0) -> int | None:
        return self.get_consumer().committed(TopicPartition(topic, partition))

    def get_end_offset(self, topic: str, partition: int = 0) -> int:
        topic_partition = TopicPartition(topic, partition)
        try:
            end_offset = self.get_consumer().end_offsets([topic_partition])[
                topic_partition
            ]
        except KeyError:
            end_offset = -1
        return end_offset

    def get_pending_messages(self, topic: str, partition: int = 0) -> int:
        last_committed = self.get_last_committed(topic, partition)
        starts = last_committed if last_committed is not None else -1
        ends = self.get_end_offset(topic, partition)
        return ends - starts


class OnMemoryService:
    group_id: str

    def __init__(self, config: Config):
        self.messages: list[ProducedMessage] = []
        self.subscripted: list[str] = []
        self.committed_messages: dict[str, int] = {}
        self.consumed_messages: list[ProducedMessage] = []

        group_id = config.group_id
        if not group_id:
            raise ImproperlyConfigured()
        self.group_id = group_id

    def subscribe(self, topics: list[str]):
        self.subscripted = topics

    def commit(self, data: CommitData | None = None):
        if data:
            self.commit_topic(data.topic)
        else:
            for topic in self.subscripted:
                self.commit_topic(topic)

    def commit_topic(self, topic: str):
        consumed_messages = [
            message for message in self.consumed_messages if message.topic == topic
        ]
        if consumed_messages:
            self.committed_messages[topic] = consumed_messages[-1].offset

    def get_messages(self) -> Iterator[ProducedMessage]:
        for message in self.messages:
            if message.topic in self.subscripted:
                self.consumed_messages.append(message)
                yield message

    def get_last_offset(self, topic: str) -> int:
        messages_in_topic = [
            message for message in self.messages if message.topic == topic
        ]
        if not messages_in_topic:
            offset = -1
        else:
            offset = messages_in_topic[-1].offset
        return offset

    def get_next_offset(self, topic: str) -> int:
        return self.get_last_offset(topic) + 1

    def produce(self, message: Message) -> ProducedMessage:
        produced_message = ProducedMessage(
            topic=message.topic,
            value=message.value,
            offset=self.get_next_offset(message.topic),
            partition=0,  # Does not allow partitions
        )
        self.messages.append(produced_message)
        return produced_message

    def get_last_committed(self, topic: str, partition: int = 0) -> int | None:
        if partition != 0:  # Does not allow partitions
            return None

        try:
            last_committed = self.committed_messages[topic]
        except KeyError:
            last_committed = None
        return last_committed

    def get_pending_messages(self, topic: str, partition: int = 0) -> int:
        if partition != 0:  # Does not allow partitions
            return 0

        last_committed = self.get_last_committed(topic, partition)
        starts = last_committed if last_committed is not None else -1
        ends = self.get_last_offset(topic)

        return ends - starts


def get_default_configuration() -> KafkaConfig:
    config = KafkaConfig()
    config.security_protocol = "PLAINTEXT"
    return config


def read_environ_configuration() -> KafkaConfig:
    config = KafkaConfig()
    config.bootstrap_servers = os.environ.get("GUIDO_HOSTS")
    config.group_id = os.environ.get("GUIDO_GROUP_ID")
    config.security_protocol = os.environ.get("GUIDO_SECURITY_PROTOCOL")
    return config


def combine_configuration(first: KafkaConfig, second: KafkaConfig) -> KafkaConfig:
    config = KafkaConfig()
    config.bootstrap_servers = (
        first.bootstrap_servers if first.bootstrap_servers else second.bootstrap_servers
    )
    config.group_id = first.group_id if first.group_id else second.group_id
    config.security_protocol = (
        first.security_protocol if first.security_protocol else second.security_protocol
    )
    return config
