import sys
import logging.config


def init_logs(loglevel, logfile):
    formatters = {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    }
    if logfile:
        handlers = {
            "default": {
                "level": loglevel,
                "formatter": "standard",
                "class": "logging.FileHandler",
                "filename": logfile,
            },
        }
    else:
        handlers = {
            "default": {
                "level": loglevel,
                "formatter": "standard",
                "class": "logging.StreamHandler",
                "stream": sys.__stderr__,
            },
        }

    config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": formatters,
        "handlers": handlers,
        "loggers": {
            "": {  # root logger
                "handlers": ["default"],
                "level": loglevel,
                "propagate": False,
            },
            "guido": {"handlers": ["default"], "level": loglevel, "propagate": False},
        },
    }
    logging.config.dictConfig(config)
