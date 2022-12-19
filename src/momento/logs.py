import logging
from typing import Optional

logger = logging.getLogger("momentosdk")


""" info('some %s stuff', 'information') """
info = logger.info


""" debug('some %s stuff', 'debug') """
debug = logger.debug


def add_logging_level(
    level_name: str, level_num: int, method_name: Optional[str] = None
) -> None:
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `level_name` becomes an attribute of the `logging` module with the value
    `level_num`. `method_name` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `method_name` is not specified, `level_name.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    >>> add_logging_level('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
        raise AttributeError("{} already defined in logging module".format(level_name))
    if hasattr(logging, method_name):
        raise AttributeError("{} already defined in logging module".format(method_name))
    if hasattr(logging.getLoggerClass(), method_name):
        raise AttributeError("{} already defined in logger class".format(method_name))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):  # type: ignore[no-untyped-def]
        if self.isEnabledFor(level_num):  # type: ignore[misc]
            self._log(level_num, message, args, **kwargs)  # type: ignore[misc]

    def logToRoot(message, *args, **kwargs):  # type: ignore[no-untyped-def]
        logging.log(level_num, message, *args, **kwargs)  # type: ignore[misc]

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, logForLevel)  # type: ignore[misc]
    setattr(logging, method_name, logToRoot)  # type: ignore[misc]


TRACE = logging.DEBUG - 5


def initialize_momento_logging() -> None:
    add_logging_level("TRACE", TRACE)
