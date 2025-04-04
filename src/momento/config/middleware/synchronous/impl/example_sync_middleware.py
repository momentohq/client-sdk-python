import time
from logging import Logger

from momento import logs
from momento.config.middleware import (
    MiddlewareMessage,
    MiddlewareRequestHandlerContext,
    MiddlewareStatus,
)
from momento.config.middleware.synchronous import Middleware, MiddlewareMetadata, MiddlewareRequestHandler


class ExampleSyncMiddleware(Middleware):
    def __init__(self) -> None:
        self._logger = logs.logger

    def on_new_request(self, context: MiddlewareRequestHandlerContext) -> MiddlewareRequestHandler:
        self._logger.info("Example synchronous middleware handling new request")
        return ExampleSyncMiddlewareRequestHandler(self._logger)


class ExampleSyncMiddlewareRequestHandler(MiddlewareRequestHandler):
    def __init__(self, logger: Logger) -> None:
        self._logger = logger

    def on_request_metadata(self, metadata: MiddlewareMetadata) -> MiddlewareMetadata:
        self._logger.info("Example synchronous middleware on_request_metadata enter")
        time.sleep(0.5)
        self._logger.info("Example synchronous middleware on_request_metadata exit")
        return metadata

    def on_request_body(self, request: MiddlewareMessage) -> MiddlewareMessage:
        self._logger.info("Example synchronous middleware on_request_body enter")
        time.sleep(0.5)
        self._logger.info("Example synchronous middleware on_request_body exit")
        return request

    def on_response_metadata(self, metadata: MiddlewareMetadata) -> MiddlewareMetadata:
        self._logger.info("Example synchronous middleware on_response_metadata enter")
        time.sleep(0.5)
        self._logger.info("Example synchronous middleware on_response_metadata exit")
        return metadata

    def on_response_body(self, response: MiddlewareMessage) -> MiddlewareMessage:
        self._logger.info("Example synchronous middleware on_response_body enter")
        time.sleep(0.5)
        self._logger.info("Example synchronous middleware on_response_body exit")
        return response

    def on_response_status(self, status: MiddlewareStatus) -> MiddlewareStatus:
        self._logger.info("Example synchronous middleware on_response_status enter")
        time.sleep(0.5)
        self._logger.info("Example synchronous middleware on_response_status exit")
        return status
