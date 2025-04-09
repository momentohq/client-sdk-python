from typing import Union

from momento.config.middleware.aio import Middleware as AsyncMiddleware
from momento.config.middleware.models import (
    MiddlewareMessage,
    MiddlewareRequestHandlerContext,
    MiddlewareStatus,
)
from momento.config.middleware.synchronous import Middleware as SyncMiddleware

Middleware = Union[SyncMiddleware, AsyncMiddleware]

__all__ = [
    "Middleware",
    "MiddlewareMessage",
    "MiddlewareStatus",
    "MiddlewareRequestHandlerContext",
]
