from typing import Callable, Union

import grpc
from grpc.aio import Metadata


class AddHeaderClientInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, headers):
        self._headers = headers

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
        for dict in self._headers:
            for header_name in dict:
                client_call_details.metadata.add(header_name, dict[header_name])

        return await continuation(client_call_details, request)
