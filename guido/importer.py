import sys
import os
import importlib
from contextlib import contextmanager


@contextmanager
def ensure_cwd_in_path():
    cwd = os.getcwd()
    if cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        yield
        sys.path.remove(cwd)


def import_app(app_path):
    full_path = app_path.split(".")
    app_path = ".".join(full_path[:-1])
    app_name = full_path[-1]

    with ensure_cwd_in_path():
        module = importlib.import_module(app_path)

    app = getattr(module, app_name)
    return app
