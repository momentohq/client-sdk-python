from socket import timeout
import grpc

import momento_wire_types.cacheclient_pb2_grpc as cache_client
import momento_wire_types.controlclient_pb2_grpc as control_client

from ._add_header_client_interceptor import AddHeaderClientInterceptor
from ._client_timeout_interceptor import ClientTimeoutInterceptor

_DEFAULT_CONTROL_CLIENT_DEADLINE_SECONDS = 60.0 # 1 minute
_DEFAULT_DATA_CLIENT_DEADLINE_SECONDS = 5.0 # 5 seconds

class _ControlGrpcManager:
    """Momento Internal."""
    def __init__(self, auth_token, endpoint):
        self._secure_channel = grpc.aio.secure_channel(
            target=endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=[
                AddHeaderClientInterceptor('authorization', auth_token),
                ClientTimeoutInterceptor(_DEFAULT_CONTROL_CLIENT_DEADLINE_SECONDS)
                ]
        )

    async def close(self):
        await self._secure_channel.close()

    def async_stub(self) -> control_client.ScsControlStub:
        return control_client.ScsControlStub(self._secure_channel)


class _DataGrpcManager:
    """Momento Internal."""

    def __init__(self, auth_token, endpoint, data_client_operation_timeout_ms):
        timeout_seconds = _DEFAULT_DATA_CLIENT_DEADLINE_SECONDS if not data_client_operation_timeout_ms else data_client_operation_timeout_ms/1000.0
        self._secure_channel = grpc.aio.secure_channel(
            target=endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=[
                AddHeaderClientInterceptor('authorization', auth_token),
                ClientTimeoutInterceptor(timeout_seconds)
                ]
        )

    async def close(self):
        await self._secure_channel.close()

    def async_stub(self) -> cache_client.ScsStub:
        return cache_client.ScsStub(self._secure_channel)
