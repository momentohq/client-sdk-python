from typing import Callable, Union

import grpc
from grpc.aio import Metadata


class AddHeaderClientInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, header_name: str, header_value: str):
        self._header_name = header_name
        self._header_value = header_value

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
        client_call_details.metadata.add(self._header_name, self._header_value)

        return await continuation(client_call_details, request)
