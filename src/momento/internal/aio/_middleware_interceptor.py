import asyncio
from types import MethodType
from typing import Awaitable, Callable, List, Optional, TypeVar, Union, cast

import grpc
from google.protobuf.message import Message
from grpc.aio import ClientCallDetails, Metadata
from grpc.aio._call import UnaryUnaryCall
from grpc.aio._typing import RequestType, ResponseType

from momento import logs
from momento.config.middleware import (
    MiddlewareMessage,
    MiddlewareRequestHandlerContext,
    MiddlewareStatus,
)
from momento.config.middleware.aio import Middleware, MiddlewareMetadata, MiddlewareRequestHandler
from momento.internal.aio._utilities import create_client_call_details, sanitize_client_call_details

T = TypeVar("T")


class _ProcessedResponseCall(UnaryUnaryCall):
    # noinspection PyMissingConstructor
    def __init__(
        self,
        call: UnaryUnaryCall,
        status_code: grpc.StatusCode = None,
        processed_response: Optional[T] = None,
        initial_metadata: Optional[Metadata] = None,
        error: Optional[grpc.RpcError] = None,
    ) -> None:
        self._call = call
        self._initial_metadata = initial_metadata
        self._status_code = status_code
        self._error = error

        # Create a future for the processed response
        self._response_future = asyncio.get_event_loop().create_future()
        if error is not None:
            self._response_future.set_exception(error)
        elif processed_response is not None:
            self._response_future.set_result(processed_response)

    async def initial_metadata(self) -> Metadata:
        if self._initial_metadata is not None:
            return self._initial_metadata
        return await self._call.initial_metadata()

    async def trailing_metadata(self) -> Metadata:
        return await self._call.trailing_metadata()

    async def code(self) -> grpc.StatusCode:
        return self._status_code

    async def details(self) -> str:
        return await self._call.details()  # type: ignore[no-any-return]

    def cancelled(self) -> bool:
        return self._call.cancelled()  # type: ignore[no-any-return]

    def done(self) -> bool:
        return True

    def time_remaining(self) -> Optional[float]:
        return self._call.time_remaining()  # type: ignore[no-any-return]

    def cancel(self) -> bool:
        return False

    def add_done_callback(self, callback) -> None:  # type: ignore[no-untyped-def]
        callback(self)

    def __await__(self):  # type: ignore[no-untyped-def]
        return self._response_future.__await__()


class MiddlewareInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    middlewares: List[Middleware]
    context: MiddlewareRequestHandlerContext

    def __init__(self, middlewares: List[Middleware], context: MiddlewareRequestHandlerContext) -> None:
        self._logger = logs.logger
        self.middlewares = middlewares
        self.context = context

    async def apply_handler_methods(self, methods: List[Callable[[T], Awaitable[T]]], original_input: T) -> T:
        current_value = original_input

        for method in methods:
            try:
                current_value = await method(current_value)
            except Exception as e:
                bound_method = cast(MethodType, method)
                handler_info = f"{bound_method.__self__.__class__.__name__}.{method.__name__}"
                self._logger.exception(f"Error in middleware method {handler_info}: {str(e)}")

        return current_value

    async def intercept_unary_unary(
        self,
        continuation: Callable[[ClientCallDetails, RequestType], UnaryUnaryCall],
        client_call_details: ClientCallDetails,
        request: RequestType,
    ) -> Union[UnaryUnaryCall, ResponseType]:
        client_call_details = sanitize_client_call_details(client_call_details)

        handlers: List[MiddlewareRequestHandler] = []
        for middleware in self.middlewares:
            handler = await middleware.on_new_request(self.context)
            handlers.append(handler)
        reversed_handlers = handlers[::-1]

        metadata = await self.apply_handler_methods(
            [handler.on_request_metadata for handler in handlers], MiddlewareMetadata(client_call_details.metadata)
        )

        new_client_call_details = create_client_call_details(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=metadata.grpc_metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )

        if isinstance(request, Message):
            middleware_message = await self.apply_handler_methods(
                [handler.on_request_body for handler in handlers], MiddlewareMessage(request)
            )
            request = middleware_message.grpc_message

        call = await continuation(new_client_call_details, request)
        try:
            initial_metadata = await call.initial_metadata()
            response_metadata = await self.apply_handler_methods(
                [handler.on_response_metadata for handler in reversed_handlers], MiddlewareMetadata(initial_metadata)
            )
            initial_metadata = response_metadata.grpc_metadata

            # if the call returns an error, awaiting it will raise an RpcError, which we handle below
            original_response = await call

            if isinstance(original_response, Message):
                middleware_response = await self.apply_handler_methods(
                    [handler.on_response_body for handler in reversed_handlers], MiddlewareMessage(original_response)
                )
                response = middleware_response.grpc_message
            else:
                response = original_response

            status_code = await call.code()
            middleware_status = await self.apply_handler_methods(
                [handler.on_response_status for handler in reversed_handlers], MiddlewareStatus(status_code)
            )
            status_code = middleware_status.grpc_status

            return _ProcessedResponseCall(call, status_code, response, initial_metadata)
        except grpc.RpcError as e:
            status = MiddlewareStatus(e.code())
            await self.apply_handler_methods([handler.on_response_status for handler in reversed_handlers], status)

            return _ProcessedResponseCall(call, e.code(), error=e)
