import sys
import importlib
import logging
import logging.config


logger = logging.getLogger("guido")


def init_logs():
    logging.config.fileConfig("logging.conf")


def main():
    init_logs()
    full_path = sys.argv[1].split(".")
    app_path = ".".join(full_path[:-1])
    app_name = full_path[-1]
    module = importlib.import_module(app_path)
    app = getattr(module, app_name)

    logger.info(f"Running {app_path}.{app_name}")
    app.run()


if __name__ == "__main__":
    main()
