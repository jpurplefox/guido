import logging
import argparse

from guido.logconfig import init_logs
from guido.importer import import_app
from guido.commands import commands


logger = logging.getLogger("guido")


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
    for command in commands.values():
        command.add_subparser(subparsers)
    return parser.parse_args()


def main():
    args = parse_args()

    init_logs(loglevel=args.loglevel, logfile=args.logfile)
    app = import_app(args.app)
    logger.info(f"Starting {args.app}")
    if not args.cmd:
        Run.run(app, args)

    commands[args.cmd].run(app, args)


if __name__ == "__main__":
    main()
