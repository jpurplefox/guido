import sys
import os
import importlib
import logging
import argparse
import json

from contextlib import contextmanager

from guido.logconfig import init_logs
from guido.messages import Message


logger = logging.getLogger("guido")


@contextmanager
def ensure_cwd_in_path():
    cwd = os.getcwd()
    if cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        yield
        sys.path.remove(cwd)


def get_app(app_path):
    full_path = app_path.split(".")
    app_path = ".".join(full_path[:-1])
    app_name = full_path[-1]

    with ensure_cwd_in_path():
        module = importlib.import_module(app_path)

    app = getattr(module, app_name)
    return app


def add_run_parser(subparsers):
    subparsers.add_parser("run", help="Runs guido to start processing messages")


def run(app):
    logger.info("Guido is running")
    app.run()


def add_produce_parser(subparsers):
    subparser = subparsers.add_parser("produce", help="Produce a message to a topic")
    subparser.add_argument("topic", help="Topic to send the message")
    subparser.add_argument("message", help="Message to be produced")


def produce(app, args):
    message = Message(topic=args.topic, value=json.loads(args.message))
    logger.info(f"Producing {message}")
    app.produce(message)


def add_pending_messages_parser(subparsers):
    subparser = subparsers.add_parser(
        "pending-messages", help="Get pending messages in a topic partition"
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


def pending_messages(app, args):
    pending_messages = app.get_pending_messages(args.topic, args.partition)
    logger.info(
        f"Pending messages for topic={args.topic} partition={args.partition}: {pending_messages}"
    )


def parse_args():
    parser = argparse.ArgumentParser(
        prog="guido",
        description="Guido is a library that simplifies the integration with Apache Kafka.",
    )
    parser.add_argument("app", help="app instance to use (e.g. module.attr_name)")
    parser.add_argument(
        "-l",
        "--loglevel",
        help="Logging level.",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
    )
    parser.add_argument(
        "-f",
        "--logfile",
        help="Path to log file. If no logfile is specified, stderr is used.",
    )
    subparsers = parser.add_subparsers(dest="cmd")
    add_run_parser(subparsers)
    add_produce_parser(subparsers)
    add_pending_messages_parser(subparsers)
    return parser.parse_args()


def main():
    args = parse_args()

    init_logs(loglevel=args.loglevel, logfile=args.logfile)
    app = get_app(args.app)
    logger.info(f"Starting {args.app}")

    match args.cmd:
        case None | "run":
            run(app)
        case "produce":
            produce(app, args)
        case "pending-messages":
            pending_messages(app, args)


if __name__ == "__main__":
    main()
