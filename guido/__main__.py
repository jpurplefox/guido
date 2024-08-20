import logging

from guido.logconfig import init_logs
from guido.importer import import_app
from guido.commands import parse_args, execute_command


logger = logging.getLogger("guido")


def main():
    args = parse_args()

    init_logs(loglevel=args.loglevel, logfile=args.logfile)
    app = import_app(args.app)
    logger.info(f"Starting {args.app}")
    execute_command(app, args)


if __name__ == "__main__":
    main()
