from types import MethodType
from typing import Callable, List, Optional, TypeVar, Union, cast

import grpc
from google.protobuf.message import Message
from grpc import StatusCode
from grpc._interceptor import _UnaryOutcome
from grpc._typing import MetadataType

from momento import logs
from momento.config.middleware import (
    MiddlewareMessage,
    MiddlewareRequestHandlerContext,
    MiddlewareStatus,
)
from momento.config.middleware.synchronous import Middleware, MiddlewareMetadata, MiddlewareRequestHandler
from momento.internal.synchronous._utilities import _ClientCallDetails, sanitize_client_call_details

RequestType = TypeVar("RequestType")
T = TypeVar("T")


class _UpdatedMetadataCall(grpc.Call):
    _call: grpc.Call
    _initial_metadata: Optional[MetadataType]
    _code: StatusCode

    def __init__(self, call: grpc.Call, initial_metadata: Optional[MetadataType], status_code: StatusCode) -> None:
        self._call = call
        self._initial_metadata = initial_metadata
        self._code = status_code

    def initial_metadata(self) -> Optional[MetadataType]:
        return self._initial_metadata

    def trailing_metadata(self) -> Optional[MetadataType]:
        return self._call.trailing_metadata()

    def code(self) -> Optional[grpc.StatusCode]:
        return self._code

    def details(self) -> Optional[str]:
        return self._call.details()  # type: ignore[no-any-return]

    def is_active(self) -> bool:
        return self._call.is_active()  # type: ignore[no-any-return]

    def time_remaining(self) -> Optional[float]:
        return self._call.time_remaining()  # type: ignore[no-any-return]

    def cancel(self) -> bool:
        return self._call.cancel()  # type: ignore[no-any-return]

    def add_callback(self, callback) -> bool:  # type: ignore[no-untyped-def]
        return self._call.add_callback(callback)  # type: ignore[no-any-return]


class MiddlewareInterceptor(grpc.UnaryUnaryClientInterceptor):
    middlewares: List[Middleware]
    context: MiddlewareRequestHandlerContext

    def __init__(self, middlewares: List[Middleware], context: MiddlewareRequestHandlerContext) -> None:
        self._logger = logs.logger
        self.middlewares = middlewares
        self.context = context

    def apply_handler_methods(self, methods: List[Callable[[T], T]], original_input: T) -> T:
        current_value = original_input

        for method in methods:
            try:
                current_value = method(current_value)
            except Exception as e:
                bound_method = cast(MethodType, method)
                handler_info = f"{bound_method.__self__.__class__.__name__}.{method.__name__}"
                self._logger.exception(f"Error in middleware method {handler_info}: {str(e)}")

        return current_value

    def intercept_unary_unary(
        self,
        continuation: Callable[[grpc.ClientCallDetails, RequestType], Union[grpc.Call, grpc.Future]],
        client_call_details: grpc.ClientCallDetails,
        request: RequestType,
    ) -> Union[grpc.Call, grpc.Future]:
        client_call_details = sanitize_client_call_details(client_call_details)

        handlers: List[MiddlewareRequestHandler] = []
        for middleware in self.middlewares:
            handler = middleware.on_new_request(self.context)
            handlers.append(handler)
        reversed_handlers = handlers[::-1]

        metadata = self.apply_handler_methods(
            [handler.on_request_metadata for handler in handlers], MiddlewareMetadata(client_call_details.metadata)
        )

        new_client_call_details = _ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=metadata.grpc_metadata,
            credentials=client_call_details.credentials,
        )

        if isinstance(request, Message):
            middleware_message = self.apply_handler_methods(
                [handler.on_request_body for handler in handlers], MiddlewareMessage(request)
            )
            request = middleware_message.grpc_message

        try:
            call = continuation(new_client_call_details, request)

            initial_metadata = call.initial_metadata()
            response_metadata = self.apply_handler_methods(
                [handler.on_response_metadata for handler in reversed_handlers], MiddlewareMetadata(initial_metadata)
            )
            initial_metadata = response_metadata.grpc_metadata

            # if the call returns an error, call.result() will raise an RpcError, which we handle below
            response_body = call.result()
            if isinstance(response_body, Message):
                middleware_message = self.apply_handler_methods(
                    [handler.on_response_body for handler in reversed_handlers], MiddlewareMessage(response_body)
                )
                response_body = middleware_message.grpc_message

            status_code = call.code()
            middleware_status = self.apply_handler_methods(
                [handler.on_response_status for handler in reversed_handlers], MiddlewareStatus(status_code)
            )
            status_code = middleware_status.grpc_status

            updated_call = _UpdatedMetadataCall(call, initial_metadata, status_code)
            updated_outcome = _UnaryOutcome(response_body, updated_call)

            return updated_outcome
        except grpc.RpcError as e:
            status = MiddlewareStatus(e.code())
            self.apply_handler_methods([handler.on_response_status for handler in reversed_handlers], status)

            raise
