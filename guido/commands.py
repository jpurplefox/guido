import json
import logging

from typing import Protocol

from guido import Guido, Message


logger = logging.getLogger("guido")


class Command(Protocol):
    name: str

    def add_subparser(self, subparser):
        ...

    def run(self, app: Guido, args):
        ...


class Run:
    name = "run"

    def add_subparser(self, subparsers):
        subparsers.add_parser(self.name, help="Runs guido to start processing messages")

    def run(self, app: Guido, args):
        logger.info("Guido is running")
        app.run()


class Produce:
    name = "produce"

    def add_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            self.name, help="Produce a message to a topic"
        )
        subparser.add_argument("topic", help="Topic to send the message")
        subparser.add_argument("message", help="Message to be produced")

    def run(self, app: Guido, args):
        message = Message(topic=args.topic, value=json.loads(args.message))
        logger.info(f"Producing {message}")
        app.produce(message)


class PendingMessages:
    name = "pending-messages"

    def add_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            self.name, help="Get pending messages in a topic partition"
        )
        subparser.add_argument("topic", help="Topic to check for pending messages")
        subparser.add_argument(
            "-p",
            "--partition",
            help="partition to check for pending messages",
            type=int,
            default=0,
            required=False,
        )

    def run(self, app: Guido, args):
        pending_messages = app.get_pending_messages(args.topic, args.partition)
        logger.info(
            f"Pending messages for topic={args.topic} partition={args.partition}: {pending_messages}"
        )


available_commands: list[Command] = [Run(), Produce(), PendingMessages()]
commands: dict[str, Command] = {command.name: command for command in available_commands}
