import abc

from momento.config.middleware.aio.middleware_metadata import MiddlewareMetadata
from momento.config.middleware.models import MiddlewareMessage, MiddlewareRequestHandlerContext, MiddlewareStatus


class MiddlewareRequestHandler(abc.ABC):
    @abc.abstractmethod
    async def on_request_metadata(self, metadata: MiddlewareMetadata) -> MiddlewareMetadata:
        pass

    @abc.abstractmethod
    async def on_request_body(self, request: MiddlewareMessage) -> MiddlewareMessage:
        pass

    @abc.abstractmethod
    async def on_response_metadata(self, metadata: MiddlewareMetadata) -> MiddlewareMetadata:
        pass

    @abc.abstractmethod
    async def on_response_body(self, response: MiddlewareMessage) -> MiddlewareMessage:
        pass

    @abc.abstractmethod
    async def on_response_status(self, status: MiddlewareStatus) -> MiddlewareStatus:
        pass


class Middleware(abc.ABC):
    @abc.abstractmethod
    async def on_new_request(self, context: MiddlewareRequestHandlerContext) -> MiddlewareRequestHandler:
        pass

    # noinspection PyMethodMayBeStatic
    def close(self) -> None:
        return None
