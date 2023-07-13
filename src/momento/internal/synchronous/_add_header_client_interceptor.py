from __future__ import annotations

import collections
from typing import Callable, TypeVar

import grpc

from momento.errors import InvalidArgumentException

RequestType = TypeVar("RequestType")
ResponseType = TypeVar("ResponseType")


class Header:
    once_only_headers = ["agent"]

    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class _ClientCallDetails(
    collections.namedtuple("_ClientCallDetails", ("method", "timeout", "metadata", "credentials")),
    grpc.ClientCallDetails,
):
    pass


class AddHeaderStreamingClientInterceptor(grpc.UnaryStreamClientInterceptor):
    are_only_once_headers_sent = False

    def __init__(self, headers: list[Header]):
        self._headers_to_add_once: list[Header] = list(
            filter(lambda header: header.name in header.once_only_headers, headers)
        )
        self.headers_to_add_every_time = list(
            filter(lambda header: header.name not in header.once_only_headers, headers)
        )

    def intercept_unary_stream(
        self,
        continuation: Callable[
            [grpc.ClientCallDetails, RequestType],
            grpc.Call,
        ],
        client_call_details: grpc.ClientCallDetails,
        request: RequestType,
    ) -> grpc.Call | ResponseType:

        new_client_call_details = sanitize_client_call_details(client_call_details)

        for header in self.headers_to_add_every_time:
            new_client_call_details.metadata.append((header.name, header.value))

        if not AddHeaderStreamingClientInterceptor.are_only_once_headers_sent:
            for header in self._headers_to_add_once:
                new_client_call_details.metadata.append((header.name, header.value))
                AddHeaderStreamingClientInterceptor.are_only_once_headers_sent = True

        return continuation(new_client_call_details, request)


class AddHeaderClientInterceptor(grpc.UnaryUnaryClientInterceptor):
    are_only_once_headers_sent = False

    @staticmethod
    def is_only_once_header(header: Header) -> bool:
        return header.name in header.once_only_headers

    @staticmethod
    def is_not_only_once_header(header: Header) -> bool:
        return header.name not in header.once_only_headers

    def __init__(self, headers: list[Header]):
        self._headers_to_add_once: list[Header] = list(filter(AddHeaderClientInterceptor.is_only_once_header, headers))
        self.headers_to_add_every_time = list(filter(AddHeaderClientInterceptor.is_not_only_once_header, headers))

    def intercept_unary_unary(
        self,
        continuation: Callable[[grpc.ClientCallDetails, RequestType], grpc.Call],
        client_call_details: grpc.ClientCallDetails,
        request: RequestType,
    ) -> grpc.Call:
        new_client_call_details = sanitize_client_call_details(client_call_details)

        for header in self.headers_to_add_every_time:
            new_client_call_details.metadata.append((header.name, header.value))

        if not AddHeaderClientInterceptor.are_only_once_headers_sent:
            for header in self._headers_to_add_once:
                new_client_call_details.metadata.append((header.name, header.value))
                AddHeaderClientInterceptor.are_only_once_headers_sent = True

        return continuation(new_client_call_details, request)


def sanitize_client_call_details(client_call_details: grpc.ClientCallDetails) -> grpc.ClientCallDetails:
    """Defensive function meant to handle inbound gRPC client request objects.

    Args:
        client_call_details: the original inbound client grpc request we are intercepting

    Returns: a new client_call_details object with metadata properly initialized to a `grpc.aio.Metadata` object
    """
    # Makes sure we can handle properly when we inject our own metadata onto request object.
    # This was mainly done as temporary fix after we observed ddtrace grpc client interceptor passing
    # client_call_details.metadata as a list instead of a grpc.aio.Metadata object.
    # See this ticket for follow-up actions to come back in and address this longer term:
    #    https://github.com/momentohq/client-sdk-python/issues/149
    # If no metadata set on passed in client call details then we are first to set, so we should just initialize
    if client_call_details.metadata is None:
        return _ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=[],
            credentials=client_call_details.credentials,
        )

    # This is block hit when ddtrace interceptor runs first and sets metadata as a list
    elif isinstance(client_call_details.metadata, list):
        return client_call_details
    else:
        # Else we raise exception for now since we don't know how to handle an unknown type
        raise InvalidArgumentException(
            "unexpected grpc client request metadata property passed to interceptor "
            "type=" + str(type(client_call_details.metadata))
        )
