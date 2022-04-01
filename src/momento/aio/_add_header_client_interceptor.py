from typing import Callable, Union, List, Dict

import grpc
from grpc.aio import Metadata


class AddHeaderClientInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    is_user_agent_sent = False

    def __init__(self, headers: List[Dict[str, str]]):
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
        if AddHeaderClientInterceptor.is_user_agent_sent == False:
            for dict in self._headers:
                for header_name in dict:
                    client_call_details.metadata.add(header_name, dict[header_name])
            AddHeaderClientInterceptor.is_user_agent_sent = True
        else:
            # Only add Authorization metadata
            header_name = list(self._headers[0].keys())[0]
            header_value = self._headers[0][header_name]
            client_call_details.metadata.add(header_name, header_value)

        return await continuation(client_call_details, request)
