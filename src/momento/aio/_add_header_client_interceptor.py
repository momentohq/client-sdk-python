from typing import Callable, Union, List

import grpc
from grpc.aio import Metadata


class Header:
    once_only_headers = ["agent"]

    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class AddHeaderClientInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    are_only_once_headers_sent = False

    def __init__(self, headers: List[Header]):
        self._headers_to_add_once = list(
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
    ) -> Union[grpc.aio._call.UnaryUnaryCall, grpc.aio._typing.ResponseType]:
        if client_call_details.metadata is None:
            client_call_details.metadata = Metadata()
        for header in self.headers_to_add_every_time:
            client_call_details.metadata.add(header.name, header.value)
        if not AddHeaderClientInterceptor.are_only_once_headers_sent:
            for header in self._headers_to_add_once:
                client_call_details.metadata.add(header.name, header.value)
            AddHeaderClientInterceptor.are_only_once_headers_sent = True

        return await continuation(client_call_details, request)
