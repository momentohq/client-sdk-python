from __future__ import annotations

from typing import Callable, TypeVar

import grpc

from momento.internal.synchronous._utilities import sanitize_client_call_details

RequestType = TypeVar("RequestType")
ResponseType = TypeVar("ResponseType")


class Header:
    once_only_headers = ["agent"]

    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class AddHeaderStreamingClientInterceptor(grpc.UnaryStreamClientInterceptor):
    def __init__(self, headers: list[Header]):
        self.are_only_once_headers_sent = False
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

        if not self.are_only_once_headers_sent:
            for header in self._headers_to_add_once:
                new_client_call_details.metadata.append((header.name, header.value))
                self.are_only_once_headers_sent = True

        return continuation(new_client_call_details, request)


class AddHeaderClientInterceptor(grpc.UnaryUnaryClientInterceptor):
    @staticmethod
    def is_only_once_header(header: Header) -> bool:
        return header.name in header.once_only_headers

    @staticmethod
    def is_not_only_once_header(header: Header) -> bool:
        return header.name not in header.once_only_headers

    def __init__(self, headers: list[Header]):
        self.are_only_once_headers_sent = False
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

        if not self.are_only_once_headers_sent:
            for header in self._headers_to_add_once:
                new_client_call_details.metadata.append((header.name, header.value))
                self.are_only_once_headers_sent = True

        return continuation(new_client_call_details, request)
