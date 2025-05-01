from __future__ import annotations

from typing import Callable

import grpc

from momento.internal.aio._utilities import sanitize_client_call_details


class Header:
    once_only_headers = ["agent", "runtime-version"]

    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class AddHeaderStreamingClientInterceptor(grpc.aio.UnaryStreamClientInterceptor):
    def __init__(self, headers: list[Header]):
        self.are_only_once_headers_sent = False
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

        if not self.are_only_once_headers_sent:
            for header in self._headers_to_add_once:
                new_client_call_details.metadata.add(header.name, header.value)
                self.are_only_once_headers_sent = True

        return await continuation(new_client_call_details, request)


class AddHeaderClientInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, headers: list[Header]):
        self.are_only_once_headers_sent = False
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

        if not self.are_only_once_headers_sent:
            for header in self._headers_to_add_once:
                new_client_call_details.metadata.add(header.name, header.value)
                self.are_only_once_headers_sent = True

        return await continuation(new_client_call_details, request)
