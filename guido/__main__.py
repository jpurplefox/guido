import importlib
import logging
import argparse

from guido.logging import init_logs


logger = logging.getLogger("guido")


def get_app(app_path):
    full_path = app_path.split(".")
    app_path = ".".join(full_path[:-1])
    app_name = full_path[-1]

    module = importlib.import_module(app_path)
    app = getattr(module, app_name)
    return app


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
    return parser.parse_args()


def main():
    args = parse_args()

    init_logs(loglevel=args.loglevel, logfile=args.logfile)
    app = get_app(args.app)

    logger.info(f"Running {args.app}")
    app.run()


if __name__ == "__main__":
    main()
