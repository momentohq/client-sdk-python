import asyncio
from logging import Logger

from momento import logs
from momento.config.middleware import (
    MiddlewareMessage,
    MiddlewareRequestHandlerContext,
    MiddlewareStatus,
)
from momento.config.middleware.aio import Middleware, MiddlewareMetadata, MiddlewareRequestHandler


class ExampleAioMiddleware(Middleware):
    def __init__(self) -> None:
        self._logger = logs.logger

    async def on_new_request(self, context: MiddlewareRequestHandlerContext) -> MiddlewareRequestHandler:
        self._logger.info("Example aio middleware handling new request")
        return ExampleMiddlewareRequestHandler(self._logger)


class ExampleMiddlewareRequestHandler(MiddlewareRequestHandler):
    def __init__(self, logger: Logger) -> None:
        self._logger = logger

    async def on_request_metadata(self, metadata: MiddlewareMetadata) -> MiddlewareMetadata:
        self._logger.info("Example aio middleware on_request_metadata enter")
        await asyncio.sleep(0.5)
        self._logger.info("Example aio middleware on_request_metadata exit")
        return metadata

    async def on_request_body(self, request: MiddlewareMessage) -> MiddlewareMessage:
        self._logger.info("Example aio middleware on_request_body enter")
        await asyncio.sleep(0.5)
        self._logger.info("Example aio middleware on_request_body exit")
        return request

    async def on_response_metadata(self, metadata: MiddlewareMetadata) -> MiddlewareMetadata:
        self._logger.info("Example aio middleware on_response_metadata enter")
        await asyncio.sleep(0.5)
        self._logger.info("Example aio middleware on_response_metadata exit")
        return metadata

    async def on_response_body(self, response: MiddlewareMessage) -> MiddlewareMessage:
        self._logger.info("Example aio middleware on_response_body enter")
        await asyncio.sleep(0.5)
        self._logger.info("Example aio middleware on_response_body exit")
        return response

    async def on_response_status(self, status: MiddlewareStatus) -> MiddlewareStatus:
        self._logger.info("Example aio middleware on_response_status enter")
        await asyncio.sleep(0.5)
        self._logger.info("Example aio middleware on_response_status exit")
        return status
