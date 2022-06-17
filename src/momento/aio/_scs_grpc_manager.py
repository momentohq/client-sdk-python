import grpc
import pkg_resources

import momento_wire_types.cacheclient_pb2_grpc as cache_client
import momento_wire_types.controlclient_pb2_grpc as control_client

from ._add_header_client_interceptor import AddHeaderClientInterceptor
from ._add_header_client_interceptor import Header


class _ControlGrpcManager:
    """Momento Internal."""

    version = pkg_resources.get_distribution("momento").version

    def __init__(self, auth_token: str, endpoint: str):
        headers = [
            Header("authorization", auth_token),
            Header("agent", f"python:{_ControlGrpcManager.version}"),
        ]
        self._secure_channel = grpc.aio.secure_channel(
            target=endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=[AddHeaderClientInterceptor(headers)],
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> control_client.ScsControlStub:
        return control_client.ScsControlStub(self._secure_channel)


class _DataGrpcManager:
    """Momento Internal."""

    version = pkg_resources.get_distribution("momento").version

    def __init__(self, auth_token: str, endpoint: str):
        headers = [
            Header("authorization", auth_token),
            Header("agent", f"python:{_ControlGrpcManager.version}"),
        ]
        self._secure_channel = grpc.aio.secure_channel(
            target=endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=[AddHeaderClientInterceptor(headers)],
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> cache_client.ScsStub:
        return cache_client.ScsStub(self._secure_channel)
