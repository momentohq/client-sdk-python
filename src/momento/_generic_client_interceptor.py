from typing import Any

import grpc
from grpc import ClientCallDetails


class _GenericClientInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, interceptor_function: Any):  # type: ignore[misc]
        self._fn = interceptor_function

    def intercept_unary_unary(  # type: ignore[misc]
        self,
        continuation: Any,
        client_call_details: ClientCallDetails,
        request: Any,
    ) -> Any:
        new_details, new_request_iterator, postprocess = self._fn(
            client_call_details, iter((request,)), False, False
        )
        response = continuation(new_details, next(new_request_iterator))
        return postprocess(response) if postprocess else response


def create(intercept_call: Any) -> _GenericClientInterceptor:  # type: ignore[misc]
    return _GenericClientInterceptor(intercept_call)
