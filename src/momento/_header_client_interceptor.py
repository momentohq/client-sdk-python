import collections
from typing import Any

import grpc
from ._generic_client_interceptor import _GenericClientInterceptor

from . import _generic_client_interceptor


class _ClientCallDetails(
    collections.namedtuple(
        "_ClientCallDetails", ("method", "timeout", "metadata", "credentials")
    ),
    grpc.ClientCallDetails,
):
    pass


def header_adder_interceptor(header: str, value: str) -> _GenericClientInterceptor:
    def intercept_call(  # type: ignore[misc]
        client_call_details: Any,
        request_iterator: Any,
        request_streaming: Any,
        response_streaming: Any,
    ) -> Any:
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)
        metadata.append(
            (
                header,
                value,
            )
        )
        client_call_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
        )
        return client_call_details, request_iterator, None

    return _generic_client_interceptor.create(intercept_call)
