import abc

from momento.config.middleware.models import MiddlewareMessage, MiddlewareRequestHandlerContext, MiddlewareStatus
from momento.config.middleware.synchronous.middleware_metadata import MiddlewareMetadata


class MiddlewareRequestHandler(abc.ABC):
    @abc.abstractmethod
    def on_request_metadata(self, metadata: MiddlewareMetadata) -> MiddlewareMetadata:
        pass

    @abc.abstractmethod
    def on_request_body(self, request: MiddlewareMessage) -> MiddlewareMessage:
        pass

    @abc.abstractmethod
    def on_response_metadata(self, metadata: MiddlewareMetadata) -> MiddlewareMetadata:
        pass

    @abc.abstractmethod
    def on_response_body(self, response: MiddlewareMessage) -> MiddlewareMessage:
        pass

    @abc.abstractmethod
    def on_response_status(self, status: MiddlewareStatus) -> MiddlewareStatus:
        pass


class Middleware(abc.ABC):
    @abc.abstractmethod
    def on_new_request(self, context: MiddlewareRequestHandlerContext) -> MiddlewareRequestHandler:
        pass

    # noinspection PyMethodMayBeStatic
    def close(self) -> None:
        return None
