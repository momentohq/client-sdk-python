from typing import Callable, Union

import grpc


class ClientTimeoutInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, request_timeout_seconds: float):
        self._request_timeout_seconds = request_timeout_seconds

    async def intercept_unary_unary(
            self,
            continuation: Callable[[grpc.aio._interceptor.ClientCallDetails, grpc.aio._typing.RequestType], grpc.aio._call.UnaryUnaryCall],
            client_call_details: grpc.aio._interceptor.ClientCallDetails,
            request: grpc.aio._typing.RequestType
    ) -> Union[grpc.aio._call.UnaryUnaryCall, grpc.aio._typing.ResponseType]:

        # Needs to be copied over, as timeout cannot be updated in place here.
        # AttributeError: can't set attribute
        new_client_call_details = grpc.aio._interceptor.ClientCallDetails(
            client_call_details.method,
            self._request_timeout_seconds,
            client_call_details.metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready)

        return await continuation(new_client_call_details, request)
