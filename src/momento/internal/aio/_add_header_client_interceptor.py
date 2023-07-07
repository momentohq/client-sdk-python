from __future__ import annotations

from typing import Callable

import grpc
from grpc.aio import ClientCallDetails, Metadata

from momento.errors import InvalidArgumentException


class Header:
    once_only_headers = ["agent"]

    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class AddHeaderStreamingClientInterceptor(grpc.aio.UnaryStreamClientInterceptor):
    are_only_once_headers_sent = False

    def __init__(self, headers: list[Header]):
        self._headers_to_add_once: list[Header] = list(
            filter(lambda header: header.name in header.once_only_headers, headers)
        )
        self.headers_to_add_every_time = list(
            filter(lambda header: header.name not in header.once_only_headers, headers)
        )

    async def intercept_unary_stream(
        self,
        continuation: Callable[
            [grpc.aio._interceptor.ClientCallDetails, grpc.aio._typing.RequestType],
            grpc.aio._call.UnaryStreamCall,
        ],
        client_call_details: grpc.aio._interceptor.ClientCallDetails,
        request: grpc.aio._typing.RequestType,
    ) -> grpc.aio._call.UnaryStreamCall | grpc.aio._typing.ResponseType:

        new_client_call_details = sanitize_client_call_details(client_call_details)

        for header in self.headers_to_add_every_time:
            new_client_call_details.metadata.add(header.name, header.value)

        if not AddHeaderStreamingClientInterceptor.are_only_once_headers_sent:
            for header in self._headers_to_add_once:
                new_client_call_details.metadata.add(header.name, header.value)
                AddHeaderStreamingClientInterceptor.are_only_once_headers_sent = True

        return await continuation(new_client_call_details, request)


class AddHeaderClientInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    are_only_once_headers_sent = False

    def __init__(self, headers: list[Header]):
        self._headers_to_add_once: list[Header] = list(
            filter(lambda header: header.name in header.once_only_headers, headers)
        )
        self.headers_to_add_every_time = list(
            filter(lambda header: header.name not in header.once_only_headers, headers)
        )

    async def intercept_unary_unary(
        self,
        continuation: Callable[
            [grpc.aio._interceptor.ClientCallDetails, grpc.aio._typing.RequestType],
            grpc.aio._call.UnaryUnaryCall,
        ],
        client_call_details: grpc.aio._interceptor.ClientCallDetails,
        request: grpc.aio._typing.RequestType,
    ) -> grpc.aio._call.UnaryUnaryCall | grpc.aio._typing.ResponseType:

        new_client_call_details = sanitize_client_call_details(client_call_details)

        for header in self.headers_to_add_every_time:
            new_client_call_details.metadata.add(header.name, header.value)

        if not AddHeaderClientInterceptor.are_only_once_headers_sent:
            for header in self._headers_to_add_once:
                new_client_call_details.metadata.add(header.name, header.value)
                AddHeaderClientInterceptor.are_only_once_headers_sent = True

        return await continuation(new_client_call_details, request)


def sanitize_client_call_details(client_call_details: grpc.aio.ClientCallDetails) -> grpc.aio.ClientCallDetails:
    """Defensive function meant to handle inbound gRPC client request objects.

    Args:
        client_call_details: the original inbound client grpc request we are intercepting

    Returns: a new client_call_details object with metadata properly initialized to a `grpc.aio.Metadata` object
    """
    # Makes sure we can handle properly when we inject our own metadata onto request object.
    # This was mainly done as temporary fix after we observed ddtrace grpc client interceptor passing
    # client_call_details.metadata as a list instead of a grpc.aio.Metadata object.
    # See this ticket for follow-up actions to come back in and address this longer term:
    # https://github.com/momentohq/client-sdk-python/issues/149
    new_client_call_details = None
    # If no metadata set on passed in client call details then we are first to set, so we should just initialize
    if client_call_details.metadata is None:
        new_client_call_details = ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=Metadata(),
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )

    # This is block hit when ddtrace interceptor runs first and sets metadata as a list
    elif isinstance(client_call_details.metadata, list):
        existing_headers = client_call_details.metadata
        metadata = Metadata()
        # re-add all existing values to new metadata
        for md_key, md_value in existing_headers:
            metadata.add(md_key, md_value)
        new_client_call_details = ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )
    elif isinstance(client_call_details.metadata, grpc.aio.Metadata):
        # If  proper grpc `grpc.aio.Metadata()` object is passed just use original object passed and pass back
        new_client_call_details = client_call_details
    else:
        # Else we raise exception for now since we don't know how to handle an unknown type
        raise InvalidArgumentException(
            "unexpected grpc client request metadata property passed to interceptor "
            "type=" + str(type(client_call_details.metadata))
        )

    return new_client_call_details
