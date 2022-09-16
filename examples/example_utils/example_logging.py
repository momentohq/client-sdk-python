import logging
import os

import colorlog
from momento.logs import initialize_momento_logging


def initialize_logging():
    initialize_momento_logging()
    debug_mode = os.getenv("DEBUG")
    trace_mode = os.getenv("TRACE")
    if trace_mode == "true":
        log_level = logging.TRACE
    elif debug_mode == "true":
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(asctime)s %(log_color)s%(levelname)-8s%(reset)s %(thin_cyan)s%(name)s%(reset)s %(message)s"
        )
    )
    handler.setLevel(log_level)
    root_logger.addHandler(handler)
