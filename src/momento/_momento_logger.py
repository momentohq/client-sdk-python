import logging

logger = logging.getLogger("momentosdk")


def info(msg: str) -> None:
    logger.info(msg)


def debug(msg: str) -> None:
    logger.debug(msg)
